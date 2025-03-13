package kvas.node.raft

import com.google.protobuf.StringValue
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasProto.*
import kvas.proto.KvasProto.PutValueResponse.StatusCode
import kvas.proto.KvasRaftProto.RaftAppendLogResponse.Status
import kvas.proto.KvasReplicationProto.LogEntry
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.proto.KvasSharedProto.DataRow
import kvas.proto.RaftReplicationServiceGrpc.RaftReplicationServiceBlockingStub
import kvas.util.KvasPool
import kvas.util.NodeAddress
import kvas.util.compareTo
import kvas.util.toLogString
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timer
import kotlin.concurrent.withLock

typealias ReplicationResult = Pair<LogEntryNumber, Status>

/**
 * Demo RAFT replication leader. This implementation simply tries to replicate new entries to the quorum, as they come,
 * and wait until either ACK or NACK from a quorum.
 */
class DemoReplicationLeader(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val dataStorage: Storage,
    private val logStorage: LogStorage
) : RaftReplicationLeader, DataServiceGrpcKt.DataServiceCoroutineImplBase() {

    private val replicationController = ReplicationController(clusterState, nodeState, dataStorage)
    private val heartbeatTimeout = timer("Heartbeat Timeout", false, HEARTBEAT_PERIOD / 2, HEARTBEAT_PERIOD) {
        if (nodeState.raftRole.value == RaftRole.LEADER) {
            replicationController.replicate()
        }
    }

    init {
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
        if (nodeState.raftRole.value != RaftRole.LEADER) {
            // Follower nodes send redirects to the leader.
            putValueResponse {
                code = StatusCode.REDIRECT
                leaderAddress = clusterState.leaderAddress.toString()
            }
        }

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
    private val dataStorage: Storage
) {
    private val log = LoggerFactory.getLogger("Raft.Leader.ReplicationController")
    private val replicationOutChannel: Channel<ReplicationResult> = Channel()
    private val replica2logSender: MutableMap<NodeAddress, RaftLogSender> = mutableMapOf()
    // Maps a log entry number to a pair of succeeded and failed replications.
    private val replicationCounter: MutableMap<LogEntryNumber, Pair<Int, Int>> = mutableMapOf()
    private val replicationLock: ReentrantLock = ReentrantLock()
    private val replicationTrigger: Condition = replicationLock.newCondition()

    private fun addReplica(replicaAddress: NodeAddress) {
        synchronized(replica2logSender) {
            replica2logSender[replicaAddress] = createLogSender(replicaAddress)
        }
    }

    private fun createLogSender(replicaAddress: NodeAddress) =
        RaftLogSender(nodeState, replicationOutChannel, replicaAddress)

    private fun start() {
        synchronized(replica2logSender) {
            replica2logSender.clear()
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
        log.info(
            "Restarted log replication. I am {}. Replicas: {}",
            nodeState.raftRole,
            replica2logSender.keys.joinToString(separator = ", ")
        )
    }

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
        // We take the results of replication of every log entry from the channel. Once a new result arrives, we update
        // the accumulated result and notify all possibly waiting requests.
        for (result in replicationOutChannel) {
            replicationLock.withLock {
                // Update the counter
                var (currentSucceeded, currentFailed) = replicationCounter[result.first] ?: Pair(0, 0)
                if (result.second == Status.OK) {
                    currentSucceeded++
                } else currentFailed++
                replicationCounter[result.first] = Pair(currentSucceeded, currentFailed)

                // And notify all waiting
                replicationTrigger.signalAll()

                // We update the last committed entry number here rather than in putValue method because it is possible that
                // the entry becomes committed when its corresponding Put request has already timed out, or even when the leader
                // restarts.
                if (currentSucceeded >= clusterState.quorumSize) {
                    this.onEntryCommitted(result.first)
                }
            }
        }
    }

    private fun onEntryCommitted(entry: LogEntryNumber) {
        if (nodeState.logStorage.lastCommittedEntryNum.value.compareTo(entry) < 0) {
            log.debug(
                "Entry {} is committed with ACKs from {}/{} replicas",
                entry.toLogString(),
                replicationCounter[entry],
                clusterState.raftNodes.size
            )
            val currentlyLastCommited = nodeState.logStorage.lastCommittedEntryNum.value
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
    private val replicaAddress: NodeAddress
) {
    private var isStopped = false
    private val log = LoggerFactory.getLogger("Raft.Leader.LogSender")
    private val logView = nodeState.logStorage.createIterator()


    private var appendLogJob: Job? = null
    val appendLogPool = KvasPool<RaftReplicationServiceBlockingStub>(NodeAddress("", 0)) {
        ManagedChannelBuilder.forAddress(it.host, it.port).usePlaintext().build().let { channel ->
            RaftReplicationServiceGrpc.newBlockingStub(channel)
        }
    }

    fun stop() {
        isStopped = true
        appendLogJob?.cancel()

    }

    fun run() {
        synchronized(this) {
            if (appendLogJob != null) {
                return
            }
            log.debug("Running LogSender={} to {}", this, replicaAddress)
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
                    log.debug("Sending a log entry {} to {}", currentEntry?.entryNumber ?: "--", replicaAddress)
                    val resp = try {
                        appendLogPool.rpc(replicaAddress) { appendLog(req) }
                    } catch (ex: Exception) {
                        log.error("Exception when sending AppendLog to {}", replicaAddress, ex)
                        raftAppendLogResponse {
                            status = Status.UNAVAILABLE
                        }
                    }
                    when (resp.status) {
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
                        Status.REJECT -> {
                            if (resp.lastCommittedEntry.compareTo(logView.lastCommittedEntry) > 0) {
                                log.debug(
                                    "Last committed entry={} at {} is newer than mine {}.",
                                    resp.lastCommittedEntry,
                                    replicaAddress,
                                    logView.lastCommittedEntry
                                )
                                if (replicaAddress != nodeState.address) {
                                    nodeState.raftRole.value = RaftRole.FOLLOWER
                                }
                            } else {
                                log.error(
                                    "Node {} rejected AppendLog request because of some other reason.",
                                    replicaAddress
                                )
                            }
                            break
                        }

                        Status.UNAVAILABLE -> {
                            log.error("Node {} is unavailable", replicaAddress)
                            break
                        }

                        else -> {
                            log.error("Received other response from node {}: {}", replicaAddress, resp)
                            break
                        }
                    }

                    // Break out of the loop if we reached the end of the log.
                    if (currentEntry == null) {
                        break
                    } else {
                        replicationOutChannel.send(currentEntry.entryNumber to resp.status)
                    }
                }
            }
            appendLogJob = job
            runBlocking {
                job.join()
            }
            log.debug("Log replication to {} completed", replicaAddress)
            appendLogJob = null
        }
    }
}

internal val replicationScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
private val LOG = LoggerFactory.getLogger("Raft.Leader")