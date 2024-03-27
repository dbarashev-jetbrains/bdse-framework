package kvas.node

import com.github.michaelbull.result.*
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kvas.proto.*
import kvas.proto.KvasProto.KvasPutResponse.StatusCode
import kvas.proto.KvasProto.LogEntry
import kvas.proto.KvasProto.LogEntryNumber
import kvas.util.KvasPool
import kvas.util.compareTo
import kvas.util.toLogString
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timer
import kotlin.concurrent.withLock
import kotlin.random.Random

// Three possible node states.
enum class RaftNodeState {
  FOLLOWER, CANDIDATE, LEADER
}

// Encapsulates information about the cluster nodes.
data class ClusterInfo(
  // The cluster size.
  val size: Int,
  // Current leader address.
  var leaderAddress: String = "",
  // The list of all cluster nodes, as reported in the ReplicaAppendLog request.
  var allNodes: List<String> = emptyList(),
  // The last moment of receiving a heartbeat timestamp.
  var heartbeatTs: Long = System.currentTimeMillis()) {
  val quorumSize = size/2 + 1

  fun isLeaderSuspectedDead() = System.currentTimeMillis() - heartbeatTs >= HEARTBEAT_PERIOD
  fun isLeaderAlive() = !isLeaderSuspectedDead()
  fun receiveHeartbeat() {
    heartbeatTs = System.currentTimeMillis()
  }
}

// Encapsulates a local log storage and the number of the last committed entry on this node.
data class LocalLogController(
  val storage: LogStorage = LogStorage(),
  var lastCommittedEntryNum: LogEntryNumber = LogEntryNumber.getDefaultInstance()
)

/**
 * This class implements a Raft cluster node.
 *
 * TODO: Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other,
 * and implement subtle details that are not directly related to Raft.
 *
 * IT IS NOT A VALID IMPLEMENTATION OF ALL ASPECTS OF THE RAFT.
 *
 */
class KvasRaftNode(selfAddress: String, initialLeaderAddress: String, clusterSize: Int) : KvasGrpcServer(selfAddress) {
  private val clusterInfo = ClusterInfo(clusterSize, initialLeaderAddress)

  // The term number of this node.
  internal var currentTerm = 0

  // Timer that initiates new leader elections.
  private val electionTimeout: Timer

  // Manages channels and stubs for RPC calls to the other cluster members.
  internal val kvasPool = KvasPool(selfAddress)

  // Access to the local Raft log storage
  internal val raftLog = LocalLogController()

  //
  private val replicationController =
    ReplicationController(quorumSize = clusterInfo.quorumSize, selfAddress = selfAddress) { outChannel, address, startEntry ->
      RaftLogSender(this, raftLog.storage.createView(), outChannel, address, startEntry)
    }.also {
      it.onEntryReplication = {entryNum ->
        logLeaderReplication.debug("Entry {} replicated to quorum and shall be committed now", entryNum.toLogString())
        if (raftLog.lastCommittedEntryNum.compareTo(entryNum) < 0) {
          raftLog.lastCommittedEntryNum = entryNum
        }
      }
    }

  // Emulates temporary outages for replicaAppendLog calls.
  // AppendLog on follower nodes will successful ~50 times and failing ~5 times
  private val replicaAppendLogOutage = OutageConfig(mode = OutageMode.SERIES, nextCountDown = createSeriesCountDown(50, 5))

  // We can trigger outages of some other calls manually.
  private val offlineOutage = OutageConfig(mode = OutageMode.AVAILABLE)

  private var isOffline: Boolean
    set(value) {
      kvasPool.isNodeOffline = value
      offlineOutage.mode = if (value) OutageMode.UNAVAILABLE else OutageMode.AVAILABLE
    }
    get() = offlineOutage.mode == OutageMode.UNAVAILABLE

  private suspend fun <T> whenOnline(code: suspend KvasRaftNode.()->T): Result<T, OutageEmulatorException> =
    whenAvailable1(offlineOutage) {
      code(this)
    }

  /**
   * Transitioning from one state to another starts or stops replication.
   */
  internal var state = if (selfAddress == initialLeaderAddress) RaftNodeState.LEADER else RaftNodeState.FOLLOWER
    set(value) {
      when (value) {
        RaftNodeState.LEADER -> {
          when (field) {
            RaftNodeState.FOLLOWER -> error("You can't transition FOLLOWER->LEADER")
            RaftNodeState.CANDIDATE -> replicationController.start(clusterInfo.allNodes, raftLog.lastCommittedEntryNum)
            RaftNodeState.LEADER -> {} // doubtful, but okay
          }
          replicaAppendLogOutage.mode = OutageMode.AVAILABLE
        }

        RaftNodeState.FOLLOWER -> {
          when (field) {
            RaftNodeState.LEADER -> {
              LoggerFactory.getLogger("Node.Election").info("This node transitions LEADER->FOLLOWER")
              replicationController.stop()

            }
            else -> {}
          }
          replicaAppendLogOutage.mode = OutageMode.SERIES
        }

        RaftNodeState.CANDIDATE -> {
          when (field) {
            RaftNodeState.LEADER -> error("You can't transition LEADER->CANDIDATE")
            else -> {}
          }
        }
      }
      field = value
    }

  private val logLeaderReplication = LoggerFactory.getLogger("Primary.Replication")
  init {
    replicationScope.launch {
      replicationController.receiveReplicationResults()
    }
    if (selfAddress != initialLeaderAddress) {
      // If this node is not the initial leader, join the leader.
      kvasPool.kvas(initialLeaderAddress) {
        replicaJoinGroup(replicaJoinGroupRequest {
          this.replicaAddress = selfAddress
        })
      }
      clusterInfo.heartbeatTs = System.currentTimeMillis()
    } else {
      runBlocking {
        replicaJoinGroup(replicaJoinGroupRequest {
          replicaAddress = selfAddress
        })
        replicationController.start(listOf(selfAddress), LogEntryNumber.getDefaultInstance())
      }
      replicaAppendLogOutage.mode = OutageMode.AVAILABLE
      currentTerm = 1
    }
    electionTimeout = timer("Election Timeout", initialDelay = HEARTBEAT_PERIOD*1.5.toLong(),
      period = Random.nextLong(HEARTBEAT_PERIOD, HEARTBEAT_PERIOD*2).also {
      LoggerFactory.getLogger("Node.Election").info("Election timeout={}", it)
    }) {
      tryElectionRound()
    }
  }

  private fun tryElectionRound() {
    // TODO: you need to write your own code instead of this.
    // Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
    // IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LEADER ELECTION PROTOCOL.

    val log = LoggerFactory.getLogger("Node.Election")
    if (clusterInfo.isLeaderAlive()) {
      // If we received the heartbeat within the timeout, we don't start the election.
      return
    }
    log.info("This node is now a CANDIDATE")
    state = RaftNodeState.CANDIDATE
    val electionRequest = leaderElectionRequest {
      requesterAddress = this@KvasRaftNode.selfAddress
      termNumber = this@KvasRaftNode.currentTerm
      lastLogEntryNumber =
        this@KvasRaftNode.raftLog.storage.lastOrNull()?.entryNumber ?: LogEntryNumber.getDefaultInstance()
    }
    val grantedVotes = mutableSetOf<String>()
    grantedVotes.add(this.selfAddress)

    log.debug("Set of nodes: {}", clusterInfo.allNodes)
    for (address in clusterInfo.allNodes) {
      if (grantedVotes.size >= clusterInfo.quorumSize) {
        // Once we have enough votes, we're done.
        break
      }
      if (clusterInfo.isLeaderSuspectedDead()) {
        try {
          kvasPool.kvas(address) {
            val response = leaderElection(electionRequest)
            if (response.isGranted) {
              grantedVotes.add(response.voterAddress)
            }
          }
        } catch (ex: Exception) {
          if (ex is StatusRuntimeException) {
            log.warn("node $address seems to be unavailable")
          } else {
            ex.printStackTrace()
          }
        }
      }
    }
    if (grantedVotes.size >= clusterInfo.quorumSize) {
      log.info("This node won elections with grants from {}", grantedVotes)
      state = RaftNodeState.LEADER
    } else {
      log.info("This node failed to win the elections, so it is now a FOLLOWER again")
      state = RaftNodeState.FOLLOWER
    }
  }

  /**
   * A node will grant its vote if its state is FOLLOWER, and it suspects the leader to be dead.
   * Otherwise it will decline the vote.
   * TODO: you need to write your own code instead of this.
   * Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
   * IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LEADER ELECTION PROTOCOL.
   */
  override suspend fun leaderElection(request: KvasProto.LeaderElectionRequest): KvasProto.LeaderElectionResponse =
    whenOnline {
      val log = LoggerFactory.getLogger("Node.Election")
      log.info("Received election request from {}", request.requesterAddress)
      if (state == RaftNodeState.FOLLOWER) {
        if (clusterInfo.isLeaderSuspectedDead()) {
          log.info("I am a FOLLOWER, and I suspect the leader={} is dead. My vote: YES", clusterInfo.leaderAddress)
          leaderElectionResponse {
            isGranted = true
            voterAddress = this@KvasRaftNode.selfAddress
            voterTermNumber = this@KvasRaftNode.currentTerm
          }
        } else {
          log.info("I am a FOLLOWER, and leader={} seems to be okay. My vote: NO", clusterInfo.leaderAddress)
          leaderElectionResponse {
            isGranted = false
            voterAddress = this@KvasRaftNode.selfAddress
            voterTermNumber = this@KvasRaftNode.currentTerm
          }
        }
      } else if (request.requesterAddress == selfAddress) {
        log.info("of course I will vote for myself")
        leaderElectionResponse {
          isGranted = true
          voterAddress = this@KvasRaftNode.selfAddress
          voterTermNumber = this@KvasRaftNode.currentTerm
        }
      } else {
        log.info("I am a {}. My vote: NO", this@KvasRaftNode.state)
        leaderElectionResponse {
          isGranted = false
          voterAddress = this@KvasRaftNode.selfAddress
          voterTermNumber = this@KvasRaftNode.currentTerm
        }
      }
    }.getOrThrow()


  // We consider "isOffline" flag which can be switched on and off to emulate
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse =
    whenOnline {
      doPutValue(request)
    }.getOrThrow()

  /**
   * Puts the value and partially replicates it.
   */
  private suspend fun doPutValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    if (state != RaftNodeState.LEADER) {
      // Follower nodes send redirects to the leader.
      return kvasPutResponse {
        code = StatusCode.REDIRECT
        leaderAddress = clusterInfo.leaderAddress
      }
    }
    // First we append the new entry to the log
    return appendLog(request).andThen {
      // Try replicating this entry synchronously to the quorum.
      replicationController.replicateToQuorum(it.entryNumber)
    }.map {
      if (it == StatusCode.OK) {
        // If replication to the quorum was successful, we're ok to apply the update.
        super.putValue(request)
      } else {
        // Otherwise we return error to the client
        kvasPutResponse {
          this.code = it
        }
      }
    }.getOrThrow()
  }

  /**
   * Processes the request to join a new replica to this replication group and initiates replicating an existing log.
   */
  override suspend fun replicaJoinGroup(request: KvasProto.ReplicaJoinGroupRequest): KvasProto.ReplicaJoinGroupResponse {
    return replicationController.addReplica(request.replicaAddress).also {
      LoggerFactory.getLogger("Primary.RegisterReplica").debug("Registered replica {}", request.replicaAddress)
    }
  }

  /**
   * Returns the list of all replicas, including the leader.
   * By convention, leader's token is 0, this distinguishes the writeable leader from the read-only followers.
   */
  override suspend fun replicaGetGroup(request: KvasProto.ReplicaGetGroupRequest): KvasProto.ReplicaGetGroupResponse {
    return replicaGetGroupResponse {
      this.replicas.addAll(replicationController.replica2logSender.entries.map { entry ->
        KvasProto.ShardInfo.newBuilder().setNodeAddress(entry.key).setShardToken(entry.value.replicaToken).build()
      })
      this.replicas.add(KvasProto.ShardInfo.newBuilder().setNodeAddress(selfAddress).setShardToken(0).build())
    }
  }

  /**
   * Appends a new log entry to the local log.
   */
  private fun appendLog(putRequest: KvasProto.KvasPutRequest): Result<LogEntry, Throwable> {
    // TODO: you need to write your own code instead of this.
    // Code written here is a prototype that keeps the whole log in-memory.
    // IT IS NOT AN IMPLEMENTATION OF THE PERSISTENT LOG.
    return synchronized(raftLog.storage) {
      val lastLogEntry = raftLog.storage.lastOrNull()
      com.github.michaelbull.result.runCatching {
        LogEntry.newBuilder()
          .setKv(KvasProto.KeyValue.newBuilder().setKey(putRequest.key).setValue(putRequest.value).build())
          .setEntryNumber(
            LogEntryNumber.newBuilder()
              .setOrdinalNumber(lastLogEntry?.entryNumber?.ordinalNumber?.let { it + 1 } ?: 1)
              .setTermNumber(currentTerm).build()
          )
          .build().also {
            raftLog.storage.add(it)
          }
      }
    }
  }

  override suspend fun replicaAppendLog(request: KvasProto.ReplicaAppendLogRequest): KvasProto.ReplicaAppendLogResponse =
    run {
      if (request.senderAddress != selfAddress) {
        whenOnline {
          doReplicaAppendLog(request)
        }
      } else {
        runCatching {
          doReplicaAppendLog(request)
        }
      }
    }.getOrThrow()

  private fun doReplicaAppendLog(request: KvasProto.ReplicaAppendLogRequest): KvasProto.ReplicaAppendLogResponse =
    try {
      // TODO: you need to write your own code instead of this.
      // Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
      // IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LOG REPLICATION PROTOCOL.
      whenAvailable(replicaAppendLogOutage) {
        val log = LoggerFactory.getLogger("Follower.AppendLog")

        clusterInfo.receiveHeartbeat()
        if (request.senderAddress == selfAddress) {
          // log.debug(".. oh, that's me pinging myself, okay")
          return@whenAvailable replicaAppendLogResponse {
            this.status = KvasProto.ReplicaAppendLogResponse.Status.OK
            this.termNumber = this@KvasRaftNode.currentTerm
          }
        }

        if (raftLog.lastCommittedEntryNum.compareTo(request.lastCommittedEntryNumber) > 0) {
          // if the last committed entry on this node has greater number than one in the request
          // then the requester is probably a former leader who lost connection with the cluster and is re-joining now.
          return@whenAvailable replicaAppendLogResponse {
            status = KvasProto.ReplicaAppendLogResponse.Status.REJECT
            termNumber = this@KvasRaftNode.currentTerm
            this.lastCommittedEntryNum = raftLog.lastCommittedEntryNum
          }
        }

        // Otherwise the request looks legitimate and we start adding the entry to the local log and updating the
        // cluster information.
        if (request.senderAddress != clusterInfo.leaderAddress) {
          log.debug("AppendLog from {}", request.senderAddress)
          log.debug(
            "Its term={} my term={}. My last log entry={}",
            request.termNumber,
            this.currentTerm,
            this.raftLog.storage.lastOrNull()?.entryNumber
          )
        }
        if (request.entriesCount != 0) {
          log.debug("from {}, log record numbers: {}", request.senderAddress, request.entriesList.map { it.entryNumber }.toList())
        }
        clusterInfo.leaderAddress = request.senderAddress
        clusterInfo.allNodes = request.replicaAddressesList
        currentTerm = request.termNumber

        // If we receive append log request while not in FOLLOWER state, switch to FOLLOWER.
        if (this.state != RaftNodeState.FOLLOWER) {
          this@KvasRaftNode.state = RaftNodeState.FOLLOWER
        }

        request.entriesList.forEach { entry ->
          raftLog.storage.add(entry)
        }

        val lastCommittedEntryNum = raftLog.lastCommittedEntryNum
        if (lastCommittedEntryNum.compareTo(request.lastCommittedEntryNumber) < 0) {
          log.info("We will commit local log entries: ({} .. {}]", lastCommittedEntryNum.toLogString(), request.lastCommittedEntryNumber.toLogString())
          // If the leader's last committed entry is > than ours, we start committing our log until we
          // reach the last committed on the server or the end of the log.
          raftLog.lastCommittedEntryNum = commitRange(lastCommittedEntryNum, request.lastCommittedEntryNumber)
          if (raftLog.lastCommittedEntryNum != request.lastCommittedEntryNumber) {
            // It is possible that the log on this node is not yet complete, because the replication is lagging.
            // A missing entry is expected to replicate soon.
            log.warn("The log on this node is not yet complete: ${raftLog.storage.toDebugString()}")
            return@whenAvailable replicaAppendLogResponse {
              this.status = KvasProto.ReplicaAppendLogResponse.Status.LOG_MISMATCH
              this.termNumber = this@KvasRaftNode.currentTerm
              this.lastCommittedEntryNum = raftLog.lastCommittedEntryNum
            }
          }
        }

        // OK response
        raftLog.lastCommittedEntryNum.let {
          replicaAppendLogResponse {
            this.status = KvasProto.ReplicaAppendLogResponse.Status.OK
            this.termNumber = this@KvasRaftNode.currentTerm
            this.lastCommittedEntryNum = raftLog.storage.createView().run {
              positionAt(it)
              get()
            }?.entryNumber ?: LogEntryNumber.getDefaultInstance()
          }
        }
      }.getOrThrow()
    } catch (ex: Exception) {
      if (ex !is OutageEmulatorException) {
        LoggerFactory.getLogger("Follower.AppendLog").error("AppendLog failed", ex)
      }
      replicaAppendLogResponse {
        this.status = KvasProto.ReplicaAppendLogResponse.Status.UNAVAILABLE
      }
    }

  /**
   * Commits the local log from the firstEntry, exclusive, and up to the last entry, inclusive.
   */
  private fun commitRange(firstEntry: LogEntryNumber, lastEntry: LogEntryNumber): LogEntryNumber {
    val log = LoggerFactory.getLogger("Follower.CommitLog")
    var lastCommitted = firstEntry
    val commitView = raftLog.storage.createView()

    commitView.positionAt(firstEntry)
    commitView.forward()
    while (true) {
      val isBreak = commitView.get()?.let {
        if (it.entryNumber.compareTo(lastEntry) > 0) {
          log.debug("Entry in the local log={} is > last committed on the leader={}, breaking", it.entryNumber.toLogString(), lastEntry.toLogString())
          true
        } else {
          log.debug("committing entry {}", it.entryNumber)
          runBlocking {
            super.putValue(kvasPutRequest {
              key = it.kv.key
              value = it.kv.value
            })
          }
          lastCommitted = it.entryNumber
          commitView.forward()
          false
        }
      } ?: true
      if (isBreak) {
        break
      }
    }
    log.info("Committed the log until entry {}", lastCommitted.toLogString())
    return lastCommitted
  }

  /**
   * Sets this node as "unavailable" for the purposes of some requests, such as log replication or leader election.
   */
  override suspend fun setOffline(request: KvasProto.KvasOfflineRequest): KvasProto.KvasOfflineResponse {
    this.isOffline = request.isOffline
    return kvasOfflineResponse {  }
  }

  // Unused for the Raft purposes.
  override fun validateToken(requestToken: Int) = true
}

/**
 * This class is responsible for the aspects of replicating log entries to the follower nodes.
 *
 * It manages the list of replicas and instances of LogSender class for each replica.
 *
 * Log senders run in parallel coroutines. However, since we can reply to putResponse one when a new log entry
 * is replicated to a quorum, the controller blocks and waits until the entry is replicated to the quorum of replicas.
 *
 * The synchronization is implemented with the help of a Kotlin channel where all log senders send the numbers of
 * the entries they replicate, and a counter of successful replications.

 * TODO: you need to modify this code as appropriate for the Raft log replication
 * Code written here is a prototype and its purpose is to provide a sketch of the log replication.
 * IT IS NOT A VALID IMPLEMENTATION OF RAFT LOG REPLICATION PROTOCOL.
 */
data class ReplicationController(
  private val replicationOutChannel: Channel<LogEntryNumber> = Channel(),
  val replica2logSender: MutableMap<String, RaftLogSender> = mutableMapOf(),
  private val replicationCounter: MutableMap<LogEntryNumber, Int> = mutableMapOf(),
  private val replicationLock: ReentrantLock = ReentrantLock(),
  private val replicationTrigger: Condition = replicationLock.newCondition(),
  private val quorumSize: Int,
  private val selfAddress: String,
  private val logSenderFactory: (outChannel: Channel<LogEntryNumber>, address: String, startEntry: LogEntryNumber)->RaftLogSender
) {

  private var heartbeatTimeout: Timer? = null
  //private var isStopped = false
  internal var onEntryReplication: (LogEntryNumber)->Unit = {}

  /**
   * Registers a new replica.
   */
  fun addReplica(replicaAddress: String, lastCommittedEntryNum: LogEntryNumber = LogEntryNumber.getDefaultInstance()): KvasProto.ReplicaJoinGroupResponse {
    synchronized(replica2logSender) {
      val replicaToken = replica2logSender.size + 1
      val logSender = logSenderFactory(replicationOutChannel, replicaAddress, lastCommittedEntryNum)
      replica2logSender[replicaAddress] = logSender
      return replicaJoinGroupResponse { this.replicaToken = replicaToken }
    }
  }

  /**
   * Creates log senders for all passed node addresses, and starts replication in a heartbeat timer.
   * The replication will start from the last committed entry on this node, if any.
   */
  fun start(allNodes: List<String>, lastCommittedEntryNum: LogEntryNumber) {
    //isStopped = false
    synchronized(replica2logSender) {
      replica2logSender.clear()
      allNodes.forEach {
        addReplica(it, lastCommittedEntryNum)
      }
    }
    heartbeatTimeout = timer("Heartbeat Timeout",false, 0, HEARTBEAT_PERIOD) {
      //if (!isStopped) {
        replicate()
      //}
    }
  }

  /**
   * Stops replication, cancels the heartbeat timer.
   */
  fun stop() {
    //this.isStopped = true
    heartbeatTimeout?.cancel()
    replica2logSender.forEach { if (it.key != selfAddress) it.value.stop() }
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
        val counter = 1 + (replicationCounter[result] ?: 0)
        replicationCounter[result] = counter

        // And notify all waiting
        replicationTrigger.signalAll()

        // We update the last committed entry number here rather than in putValue method because it is possible that
        // the entry becomes committed when its corresponding Put request has already timed out, or even when the leader
        // restarts.
        if (counter >= quorumSize) {
          this.onEntryReplication(result)
        }
      }
    }
  }

  /**
   * This function start replication of the given log entry and blocks until it is replicated to the quorum.
   */
  internal fun replicateToQuorum(logEntry: LogEntryNumber): Result<StatusCode, Throwable> {
    replicate()
    while (true) {
      replicationLock.withLock {
        // We are waiting for at least quorumSize messages indicating that logEntry was successfully replicated.
        if (replicationCounter.getOrDefault(logEntry, 0) >= quorumSize) {
          return Ok(StatusCode.OK)
        } else {
          // Wait if we have not yet received enough acks.
          replicationTrigger.await()
        }
      }
    }
  }

  private fun replicate() {
    replica2logSender.values.forEach { it.run() }
  }
}

/**
 * This class is responsible for the log replication process.
 */
class RaftLogSender(
  private val server: KvasRaftNode,
  private val logView: RaftLogView,
  private val replicationOutChannel: Channel<LogEntryNumber>,
  private val replicaAddress: String,
  startEntry: LogEntryNumber
) : LogSender(0) {

  private val log = LoggerFactory.getLogger("Primary.AppendLog")
  private var appendLogJob: Job? = null

  init {
    logView.positionAt(startEntry)
    logView.forward()
  }

  fun stop() {
    appendLogJob?.cancel()
  }

  override fun run() {
    log.debug("Running LogSender to {}", replicaAddress)
    appendLogJob = replicationScope.launch {
      // TODO: you need to write your own code.
      // This code is a simplified prototype and is not a valid implementation of the Raft log replication.
      while (true) {
        val entry = logView.get()
        val req = replicaAppendLogRequest {
          entry?.let {entries.add(it)}
          termNumber = server.currentTerm
          lastCommittedEntryNumber = server.raftLog.lastCommittedEntryNum
          replicaAddresses.addAll(server.kvasPool.nodes.keys)
          senderAddress = server.selfAddress
        }
        //log.debug("Sending a log entry {} to {}", entry?.entryNumber ?: "--", replicaAddress)
        val resp = try {
          server.kvasPool.kvas(replicaAddress) {
            replicaAppendLog(req)
          }
        } catch (ex: Exception) {
          log.error("Exception when sending AppendLog to {}", replicaAddress, ex)
          replicaAppendLogResponse {
            status = KvasProto.ReplicaAppendLogResponse.Status.UNAVAILABLE
          }
        }
        when (resp.status) {
          KvasProto.ReplicaAppendLogResponse.Status.OK -> {
            if (entry != null) {
              log.debug("Entry {} replicated to {}", entry.entryNumber.toLogString(), replicaAddress)
              replicationOutChannel.send(entry.entryNumber)
              logView.forward()
            }
          }
          KvasProto.ReplicaAppendLogResponse.Status.LOG_MISMATCH -> {
            logView.positionAt(resp.lastCommittedEntryNum)
            logView.forward()

          }
          KvasProto.ReplicaAppendLogResponse.Status.REJECT -> {
            if (resp.lastCommittedEntryNum.compareTo(server.raftLog.lastCommittedEntryNum) > 0) {
              server.state = RaftNodeState.FOLLOWER
            } else {
              log.error("Node {} rejected AppendLog request because of some other reason.", replicaAddress)
            }
            break
          }
          KvasProto.ReplicaAppendLogResponse.Status.UNAVAILABLE -> {
            log.error("Node {} is unavailable", replicaAddress)
            break
          }
          else -> {
            log.error("Received other response from node {}: {}", replicaAddress, resp)
            break
          }
        }
        // Break out of the loop if we reached the end of the log.
        if (entry == null) {
          break
        }
      }
    }
    log.debug("Log replication to {} completed", replicaAddress)
  }
}

class RaftLogView(private val entries: List<LogEntry>, private var pos: Int = 0) {
  fun get(): LogEntry? = if (pos >= 0 && entries.size > pos) entries[pos] else null

  fun forward(): Boolean {
    if (pos < entries.size) pos++
    return pos < entries.size
  }

  fun positionAt(entry: LogEntryNumber) {
    if (entry == LogEntryNumber.getDefaultInstance()) {
      pos = -1
      return
    }
    val idx = entries.indexOfLast {
      it.entryNumber == entry
    }
    if (idx != -1) {
      pos = idx
    }
  }
}


class LogStorage {
  private val entries = mutableListOf<LogEntry>()

  fun lastOrNull(): LogEntry? = entries.lastOrNull()

  fun add(entry: LogEntry) {
    synchronized(entries) {
      entries.add(entry)
    }
  }

  fun createView() = RaftLogView(entries)
  fun toDebugString(): String =
    entries.map { it.entryNumber.toLogString() }.joinToString(separator = ", ")

}

private val replicationScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
private const val HEARTBEAT_PERIOD = 2000L
