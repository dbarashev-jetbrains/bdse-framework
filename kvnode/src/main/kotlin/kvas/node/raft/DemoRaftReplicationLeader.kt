package kvas.node.raft

import com.google.protobuf.StringValue
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kvas.node.storage.ClusterOutageState
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasProto.*
import kvas.proto.KvasProto.PutValueResponse.StatusCode
import kvas.proto.KvasRaftProto.RaftAppendLogResponse.Status
import kvas.proto.KvasReplicationProto.LogEntry
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.proto.KvasSharedProto.DataRow
import kvas.proto.RaftReplicationServiceGrpc.RaftReplicationServiceBlockingStub
import kvas.util.*
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timer
import kotlin.concurrent.withLock

data class ReplicationResult(val entry: LogEntryNumber, val status: Status, val replica: NodeAddress)
/**
 * Demo RAFT replication leader. This implementation simply tries to replicate new entries to the quorum, as they come,
 * and wait until either ACK or NACK from a quorum.
 *
 * This is a part of the DEMO RaftReplicationLeader implementation.
 * You may use it as a reference or a starting point of your own implementation of the correct RAFT replication protocol.
 */
class DemoReplicationLeader(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val dataStorage: Storage,
    private val logStorage: LogStorage,
    private val clusterOutageState: ClusterOutageState
) : RaftReplicationLeader, DataServiceGrpcKt.DataServiceCoroutineImplBase() {

    private val replicationController = ReplicationController(clusterState, nodeState, dataStorage, clusterOutageState)

    init {
        timer("Heartbeat Timeout", false, HEARTBEAT_PERIOD / 2, HEARTBEAT_PERIOD) {
            if (nodeState.raftRole.value == RaftRole.LEADER) {
                replicationController.replicate()
            }
        }
        nodeState.raftRole.subscribe { oldRole, newRole ->
            if (newRole == RaftRole.LEADER && oldRole == RaftRole.CANDIDATE) {
                replicationController.restart()
            }
            if (newRole == RaftRole.FOLLOWER && oldRole == RaftRole.LEADER) {
                replicationController.stop()
            }
            true
        }

        replicationScope.launch {
            replicationController.receiveReplicationResults()
        }
        replicationController.restart()
    }

    /**
     * GET returns values directly from the data storage.
     */
    override suspend fun getValue(request: GetValueRequest): GetValueResponse = try {
        val value = dataStorage.get(request.rowKey, request.columnName)
        getValueResponse {
            if (value != null) {
                this.value = StringValue.of(value)
            }
            this.code = GetValueResponse.StatusCode.OK
        }
    } catch (ex: Throwable) {
        LOG.error("Failed to GET key={}", request.rowKey, ex)
        getValueResponse {
            this.code = GetValueResponse.StatusCode.STORAGE_ERROR
        }
    }

    /**
     * PUT protocol:
     * 1. Append a new entry to the log.
     * 2. Request the replication controller to replicate a new entry and wait for the result.
     * 3. If the controller successfully replicates to a quorum, the entry will become committed and will be applied
     *    to the local data storage.
     *
     * The response status code is OK if a new entry was successfully replicated and committed, otherwise
     * other codes are used.
     */
    override suspend fun putValue(request: PutValueRequest): PutValueResponse = try {
        if (clusterOutageState.isAvailable.not()) {
            putValueResponse {
                code = StatusCode.REFRESH_SHARDS
            }
        } else if (nodeState.raftRole.value != RaftRole.LEADER) {
            // Follower nodes send redirects to the leader.
            putValueResponse {
                code = StatusCode.REDIRECT
                leaderAddress = clusterState.leaderAddress.toString()
            }
        } else {
            // We synchronize on the service instance to make sure that requests are placed into the log and replicated
            // in the same order.
            synchronized(this) {
                // First we create a new entry...
                val logEntry = createLogEntry(request)
                // ... and then try replicating it to the quorum.
                // The replicate call is synchronous, in the sense that it will not return until we receive responses from
                // the quorum.
                val replicationStatus: StatusCode = replicationController.replicateToQuorum(logEntry.entryNumber)

                if (replicationStatus == StatusCode.OK) {
                    // In case of success, we are okay to write the update to the storage and reply OK to the client.
                    dataStorage.put(request.rowKey, request.columnName, request.value)
                    putValueResponse {
                        code = StatusCode.OK
                    }
                } else {
                    // Otherwise we return error to the client
                    putValueResponse {
                        this.code = replicationStatus
                    }
                }
            }
        }
    } catch (ex: Throwable) {
        LOG.error("Failed to PUT key={}", request.rowKey, ex)
        putValueResponse {
            this.code = StatusCode.STORAGE_ERROR
        }
    }

    /**
     * Appends a new log entry to the local log.
     */
    private fun createLogEntry(putRequest: PutValueRequest): LogEntry {
        return synchronized(nodeState) {
            val lastLogEntry = logStorage.lastOrNull()
            val newEntry = LogEntry.newBuilder()
                .setDataRow(
                    DataRow.newBuilder().setKey(putRequest.rowKey).putValues(putRequest.columnName, putRequest.value)
                )
                .setEntryNumber(
                    LogEntryNumber.newBuilder()
                        .setOrdinalNumber(lastLogEntry?.entryNumber?.ordinalNumber?.let { it + 1 } ?: 1)
                        .setTermNumber(nodeState.currentTerm).build()
                )
                .build()
            LOG.debug("LEADER: added ${newEntry.entryNumber.toLogString()}")
            logStorage.add(newEntry)
            newEntry
        }
    }

    override fun getDataService(): DataServiceGrpcKt.DataServiceCoroutineImplBase {
        return this
    }

    override fun onMetadataChange() {
        replicationController.restart()
    }
}

/**
 * This class is responsible for the parallel log replication to all replicas. It uses the LogSender instances
 * for replicating to particular nodes, collects replication status from them using a Channel and keeps an in-memory map
 * of the log entry numbers to the count of nodes where it was replicated.
 *
 * This is a part of the DEMO RaftReplicationLeader implementation.
 * You may use it as a reference or a starting point of your own implementation of the correct RAFT replication protocol.
 */
class ReplicationController(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val dataStorage: Storage,
    private val clusterOutageState: ClusterOutageState
) {
    private val LOGGING = LoggerFactory.getLogger("Raft.Leader.ReplicationController")
    // We will receive replication results through this channel.
    private val replicationOutChannel: Channel<ReplicationResult> = Channel()
    // A map of all log senders.
    private val replica2logSender: MutableMap<NodeAddress, RaftLogSender> = mutableMapOf()
    // Maps a log entry number to a pair of succeeded and failed replications
    private val replicationCounter: MutableMap<LogEntryNumber, Pair<Int, Int>> = mutableMapOf()
    private val replicationLock: ReentrantLock = ReentrantLock()
    private val replicationTrigger: Condition = replicationLock.newCondition()
    private val unavailableNodes = mutableSetOf<NodeAddress>()

    private fun addReplica(replicaAddress: NodeAddress) {
        synchronized(replica2logSender) {
            replica2logSender[replicaAddress] = createLogSender(replicaAddress)
        }
    }

    private fun createLogSender(replicaAddress: NodeAddress) =
        RaftLogSender(nodeState, replicationOutChannel, replicaAddress, clusterOutageState)

    private fun start() {
        synchronized(replica2logSender) {
            replica2logSender.clear()
            replicationCounter.clear()
            clusterState.raftNodes.forEach { addReplica(it) }
        }
    }

    fun stop() {
        synchronized(replica2logSender) {
            replica2logSender.values.forEach { it.stop() }
        }
    }

    fun restart() {
        synchronized(replica2logSender) {
            stop()
            start()
        }
        LOGGING.info(
            "Restarted log replication. I am {}. Replicas: {}",
            nodeState.raftRole,
            replica2logSender.keys.joinToString(separator = ", ")
        )
    }

    /**
     * This method starts the log senders and waits until a quorum of replicas acknowledges or rejects the entry replication.
     */
    internal fun replicateToQuorum(logEntry: LogEntryNumber): StatusCode {
        replicate()
        try {
            while (true) {
                replicationLock.withLock {
                    // We are waiting for at least quorumSize messages indicating that logEntry was successfully replicated.
                    val currentResult = replicationCounter.getOrDefault(logEntry, 0 to 0)
                    if (currentResult.first >= clusterState.quorumSize) {
                        return StatusCode.OK
                    } else if (currentResult.second >= clusterState.quorumSize) {
                        return StatusCode.COMMIT_FAILED
                    } else {
                        // Wait if we have not yet received enough replies.
                        replicationTrigger.await()
                    }
                }
            }
        } catch (ex: Throwable) {
            LOG.error("Exception when waiting for replication results", ex)
            return StatusCode.STORAGE_ERROR
        }
    }

    fun replicate() {
        replica2logSender.values.forEach { it.run() }
    }

    /**
     * This function listens to the messages coming from the log senders via the channel and increments counters associated
     * with each log entry. After incrementing the counter we send a signal to all request that are possibly waiting
     * until the end of replication to the quorum.
     */
    internal suspend fun receiveReplicationResults() {
        // Every log sender reports the replication results to this channel. Once a new result arrives, we update
        // the accumulated result and notify all possibly waiting requests.
        for (result in replicationOutChannel) {
            replicationLock.withLock {
                // Update the counter
                if (result.entry == LogEntryNumber.getDefaultInstance() && result.status == Status.UNAVAILABLE) {
                    unavailableNodes.add(result.replica)
                    if (unavailableNodes.size == clusterState.quorumSize) {
                        LOG.debug("More than {} replicas are unavailable. Transitioning to FOLLOWER", clusterState.quorumSize)
                        nodeState.raftRole.value = RaftRole.FOLLOWER
                    }
                } else {
                    unavailableNodes.remove(result.replica)
                    LOG.debug("Received result {}", result)
                    var (currentSucceeded, currentFailed) = replicationCounter[result.entry] ?: Pair(0, 0)
                    if (result.status == Status.OK) {
                        currentSucceeded++
                    } else currentFailed++
                    replicationCounter[result.entry] = Pair(currentSucceeded, currentFailed)

                    // And notify all waiting
                    replicationTrigger.signalAll()

                    if (currentSucceeded >= clusterState.quorumSize) {
                        // If replication is successful, we need to check if this value needs to be committed.
                        this.onEntryCommitted(result.entry)
                        LOG.debug("Entry {} replicated to quorum", result.entry.toLogString())
                    } else if (currentFailed >= clusterState.quorumSize) {
                        // This is a demo implementation, so if we fail to replicate to the quorum, we give up and become
                        // a follower. This may not be necessary in the real leader implementation though.
                        LOG.warn("Entry {} failed to replicate to quorum", result.entry.toLogString())
                        nodeState.raftRole.value = RaftRole.FOLLOWER
                    } else {
                        LOG.debug("Received replies from {} replicas... need more", currentSucceeded + currentFailed)
                    }
                }
            }
        }
    }

    /**
     * Processes the event of committing an entry. We check if the entry number is greater than the number of
     * the last committed entry and if this is the case, commit the whole range.
     */
    private fun onEntryCommitted(entry: LogEntryNumber) {
        val logStorage = nodeState.logStorage
        if (logStorage.lastCommittedEntryNum.value.compareTo(entry) < 0) {
            LOGGING.debug(
                "Entry {} is committed with ACKs from {}/{} replicas",
                entry.toLogString(),
                replicationCounter[entry],
                clusterState.raftNodes.size
            )
            val currentlyLastCommited = logStorage.lastCommittedEntryNum.value
            nodeState.logStorage.commitRange(dataStorage, currentlyLastCommited, entry)
        }
    }
}

/**
 * This class is responsible for replicating the log to a single replica.
 *
 * This is a part of the DEMO RaftReplicationLeader implementation.
 * You may use it as a reference or a starting point of your own implementation of the correct RAFT replication protocol.
 */
class RaftLogSender(
    private val nodeState: NodeState,
    private val replicationOutChannel: Channel<ReplicationResult>,
    private val replicaAddress: NodeAddress,
    private val clusterOutageState: ClusterOutageState
) {
    private var isStopped = false
    private val log = LoggerFactory.getLogger("Raft.Leader.LogSender")
    // This log sender will replicate entries using its own log iterator instance.
    private val logView = nodeState.logStorage.createIterator()


    private var appendLogJob: Job? = null
    val appendLogPool = GrpcPoolImpl<RaftReplicationServiceBlockingStub>(NodeAddress("", 0)) {
         channel -> RaftReplicationServiceGrpc.newBlockingStub(channel)
    }

    fun stop() {
        isStopped = true
        appendLogJob?.cancel()

    }

    /**
     * Calling run() will start replication from the current position of the log iterator.
     * It should work fine, assuming that all entries until the current one have been successfully replicated before,
     * and now we're replicating the remaining suffix.
     */
    fun run() {
        // We don't want more than one replication job per replica running at any moment.
        synchronized(this) {
            if (appendLogJob != null) {
                return
            }
            log.debug("Running {}", this)
            val job = replicationScope.launch {
                while (!isStopped) {
                    val currentEntry = logView.get()
                    val req = raftAppendLogRequest {
                        if (currentEntry != null) {
                            entry = currentEntry
                        }
                        lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                        termNumber = nodeState.currentTerm
                        senderAddress = nodeState.address.toString()
                    }

                    // We check if the call is allowed by the outage emulator.
                    val resp = try {
                        // It is possible that this node is "offline"
                        if (!clusterOutageState.isAvailable) {
                            throw StatusRuntimeException(io.grpc.Status.UNAVAILABLE)
                        }
                        // It is possible that there is "no connection" between this node and the replica.
                        if (clusterOutageState.unavailableNodes.contains(replicaAddress)) {
                            throw StatusRuntimeException(io.grpc.Status.UNAVAILABLE)
                        }
                        appendLogPool.rpc(replicaAddress) { appendLog(req) }
                    } catch (ex: Exception) {
                        log.error("Exception when sending AppendLog to {}", replicaAddress, ex)
                        raftAppendLogResponse {
                            status = Status.UNAVAILABLE
                        }
                    }

                    log.debug("....{} result for entry {}", resp.status, currentEntry?.entryNumber ?: "--")
                    // Now let's analyze some of the call statuses
                    when (resp.status) {
                        // If replication completed OK, we advance the log iterator and will start replicating
                        // the next entry.
                        Status.OK -> {
                            if (currentEntry != null) {
                                log.debug(
                                    "Entry {} replicated to {}",
                                    currentEntry.entryNumber.toLogString(),
                                    replicaAddress
                                )
                                logView.advance()
                            }
                        }
                        // If replica rejected our entry, we check if the last committed entry on the replica is
                        // more recent than ours. In this case we need to give up and become a follower, as
                        // it is clear that there is another leader who committed more recent entries.
                        Status.REJECT -> {
                            val lastCommitted = nodeState.logStorage.lastCommittedEntryNum.value
                            if (resp.lastCommittedEntry.compareTo(lastCommitted) > 0) {
                                log.debug(
                                    "Last committed entry={} at {} is newer than mine {}.",
                                    resp.lastCommittedEntry,
                                    replicaAddress,
                                    lastCommitted
                                )
                                if (replicaAddress != nodeState.address) {
                                    nodeState.raftRole.value = RaftRole.FOLLOWER
                                }
                            } else {
                                // However, since it is a demo, we will transition to FOLLOWER state just because some
                                // node rejects our AppendLog. It may not be necessary in the real implementation.
                                log.error(
                                    "Node {} rejected AppendLog request because of some other reason.",
                                    replicaAddress
                                )
                                if (replicaAddress != nodeState.address) {
                                    nodeState.raftRole.value = RaftRole.FOLLOWER
                                }
                            }
                        }

                        // Otherwise we do nothing, because it is a demo implementation.
                        Status.UNAVAILABLE -> {
                            log.error("Node {} is unavailable", replicaAddress)
                        }

                        else -> {
                            log.error("Received other response from node {}: {}", replicaAddress, resp)
                        }
                    }

                    if (currentEntry != null || resp.status == Status.UNAVAILABLE) {
                        // We report the replication status to the channel, so that in case of success
                        // the controller could continue.
                        replicationOutChannel.send(ReplicationResult(entry = currentEntry?.entryNumber ?: LogEntryNumber.getDefaultInstance(), status = resp.status, replica = replicaAddress))
                    }

                    if (currentEntry == null || resp.status != Status.OK) {
                        // We will break out of the loop if we reached the end of the log, or if replication has not been successful.
                        break
                    }
                }
            }
            appendLogJob = job
            runBlocking {
                job.join()
            }
            appendLogJob = null
        }
    }

    override fun toString(): String {
        return "LogSender(replicaAddress=$replicaAddress)"
    }


}

internal val replicationScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
private val LOG = LoggerFactory.getLogger("Raft.Leader")