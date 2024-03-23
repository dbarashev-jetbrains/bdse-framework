package kvas.node

import com.github.michaelbull.result.*
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
import kotlin.concurrent.scheduleAtFixedRate
import kotlin.concurrent.timer
import kotlin.concurrent.withLock
import kotlin.random.Random

enum class RaftNodeState {
  FOLLOWER, CANDIDATE, LEADER
}

data class ClusterInfo(
  val size: Int,
  var leaderAddress: String = "",
  var allNodes: List<String> = emptyList(),
  var heartbeatTs: Long = System.currentTimeMillis()) {
  val quorumSize = size/2 + 1

  fun isLeaderSuspectedDead() = System.currentTimeMillis() - heartbeatTs >= HEARTBEAT_PERIOD*1.5
  fun isLeaderAlive() = !isLeaderSuspectedDead()
  fun receiveHeartbeat() {
    heartbeatTs = System.currentTimeMillis()
  }
}

data class LocalLogController(
  val storage: LogStorage = LogStorage(),
  var lastCommittedEntryNum: LogEntryNumber = LogEntryNumber.getDefaultInstance()
)

class KvasRaftNode(selfAddress: String, initialLeaderAddress: String, clusterSize: Int) : KvasGrpcServer(selfAddress) {
  private val electionTimeout: Timer

  private val clusterInfo = ClusterInfo(clusterSize, initialLeaderAddress)
  internal var currentTerm = 0
  internal val kvasPool = KvasPool()
  private val outageEmulator = OutageEmulator<KvasProto.ReplicaAppendLogResponse>()
  internal val raftLog = LocalLogController()
  internal val replicationController =
    ReplicationController(quorumSize = clusterInfo.quorumSize) { outChannel, address ->
      RaftLogSender(this, raftLog.storage.createView(), outChannel, address)
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
            RaftNodeState.CANDIDATE -> replicationController.start(clusterInfo.allNodes)
            RaftNodeState.LEADER -> {} // doubtful, but okay
          }
          outageEmulator.disabled = true
        }

        RaftNodeState.FOLLOWER -> {
          when (field) {
            RaftNodeState.LEADER -> replicationController.stop()
            else -> {}
          }
          outageEmulator.disabled = false
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
      replicationController.receiveReplicationResults { lastCommittedEntryNum ->
        logLeaderReplication.debug("Entry {} replicated to quorum and shall be committed now", lastCommittedEntryNum)
        raftLog.lastCommittedEntryNum = lastCommittedEntryNum
      }
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
        replicationController.start(listOf(selfAddress))
      }
      outageEmulator.disabled = true
      currentTerm = 1
    }
    electionTimeout = timer("Election Timeout", initialDelay = HEARTBEAT_PERIOD*1.5.toLong(),
      period = Random.nextLong(HEARTBEAT_PERIOD, HEARTBEAT_PERIOD*2).also {
      println("Election timeout=$it")
    }) {
      tryElectionRound()
    }
  }

  private fun tryElectionRound() {
    // TODO: you need to write your own code instead of this.
    // Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
    // IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LEADER ELECTION PROTOCOL.

    if (clusterInfo.isLeaderAlive()) {
      // If we received the heartbeat within the timeout, we don't start the election.
      return
    }
    println("This node is now a CANDIDATE")
    state = RaftNodeState.CANDIDATE
    val electionRequest = leaderElectionRequest {
      requesterAddress = this@KvasRaftNode.selfAddress
      termNumber = this@KvasRaftNode.currentTerm
      lastLogEntryNumber =
        this@KvasRaftNode.raftLog.storage.lastOrNull()?.entryNumber ?: LogEntryNumber.getDefaultInstance()
    }
    val grantedVotes = mutableSetOf<String>()
    grantedVotes.add(this.selfAddress)

    for ((_, stub) in kvasPool.nodes) {
      if (grantedVotes.size >= clusterInfo.quorumSize) {
        // Once we have enough votes, we're done.
        break
      }
      if (clusterInfo.isLeaderSuspectedDead()) {
        val response = stub.leaderElection(electionRequest)
        if (response.isGranted) {
          grantedVotes.add(response.voterAddress)
        }
      }
    }
    if (grantedVotes.size >= clusterInfo.quorumSize) {
      println("This node won elections with grants from $grantedVotes")
      state = RaftNodeState.LEADER
    } else {
      println("This node failed to win the elections, so it is now a FOLLOWER again")
      state = RaftNodeState.FOLLOWER
    }
  }

  /**
   * A node will grant its vote if its state is FOLLOWER, otherwise it will decline the vote.
   */
  override suspend fun leaderElection(request: KvasProto.LeaderElectionRequest): KvasProto.LeaderElectionResponse {
    // TODO: you need to write your own code instead of this.
    // Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
    // IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LEADER ELECTION PROTOCOL.

    println("Received election request from ${request.requesterAddress}")
    return if (state == RaftNodeState.FOLLOWER) {
      println("I am a follower, so I grant my vote")
      leaderElectionResponse {
        isGranted = true
        voterAddress = this@KvasRaftNode.selfAddress
        voterTermNumber = this@KvasRaftNode.currentTerm
      }
    } else if (request.requesterAddress == selfAddress) {
      println("of course I will vote for myself")
      leaderElectionResponse {
        isGranted = true
        voterAddress = this@KvasRaftNode.selfAddress
        voterTermNumber = this@KvasRaftNode.currentTerm
      }
    } else {
      println("I am a ${this@KvasRaftNode.state}, so I decline my vote")
      leaderElectionResponse {
        isGranted = false
        voterAddress = this@KvasRaftNode.selfAddress
        voterTermNumber = this@KvasRaftNode.currentTerm
      }
    }
  }

  /**
   */
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    if (state != RaftNodeState.LEADER) {
      return kvasPutResponse {
        code = StatusCode.REDIRECT
        leaderAddress = clusterInfo.leaderAddress
      }
    }
    println("putValue: $request")
    return appendLog(request).andThen {
      // Try replicating this entry.
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
    return replicationController.addReplica(request.replicaAddress)
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

  private fun appendLog(putRequest: KvasProto.KvasPutRequest): Result<LogEntry, Throwable> {
    // TODO: you need to write your own code instead of this.
    // Code written here is a prototype that keeps the whole log in-memory.
    // IT IS NOT AN IMPLEMENTATION OF THE PERSISTENT LOG.
    return synchronized(raftLog.storage) {
      val lastLogEntry = raftLog.storage.lastOrNull()
      println("last in storage=$lastLogEntry")
      com.github.michaelbull.result.runCatching {
        LogEntry.newBuilder()
          .setKv(KvasProto.KeyValue.newBuilder().setKey(putRequest.key).setValue(putRequest.value).build())
          .setEntryNumber(
            LogEntryNumber.newBuilder()
              .setOrdinalNumber(lastLogEntry?.entryNumber?.ordinalNumber?.let { it + 1 } ?: 1)
              .setTermNumber(currentTerm).build()
          )
          .build().also {
            println("new entry=$it")
            raftLog.storage.add(it)
          }
      }
    }
  }

  override suspend fun replicaAppendLog(request: KvasProto.ReplicaAppendLogRequest): KvasProto.ReplicaAppendLogResponse =
    try {
      // TODO: you need to write your own code instead of this.
      // Code written here is a prototype and its purpose is to sketch the way nodes communicate with each other.
      // IT IS NOT A VALID IMPLEMENTATION OF THE RAFT LOG REPLICATION PROTOCOL.
      outageEmulator.serveIfAvailable {
        val log = LoggerFactory.getLogger("Follower.AppendLog")

        clusterInfo.receiveHeartbeat()
        if (request.senderAddress == selfAddress) {
          // log.debug(".. oh, that's me pinging myself, okay")
          return@serveIfAvailable replicaAppendLogResponse {
            this.status = KvasProto.ReplicaAppendLogResponse.Status.OK
            this.termNumber = this@KvasRaftNode.currentTerm
          }
        }

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
        if (lastCommittedEntryNum.compareTo(request.lastCommittedEntryNumber) > 0) {
          // if the last committed entry on this node has greater number than one in the request
          // then we are in troubles.
          error("The last committed entry on node=${this.selfAddress} is $lastCommittedEntryNum > the last committed entry on the leader=${request.senderAddress}: ${request.lastCommittedEntryNumber}")
        } else if (lastCommittedEntryNum.compareTo(request.lastCommittedEntryNumber) < 0) {
          log.info("We will commit local log entries: ({} .. {}]", lastCommittedEntryNum.toLogString(), request.lastCommittedEntryNumber.toLogString())
          // Otherwise, if the leader's last committed entry is > than ours, we start committing our log until we
          // reach the last committed on the server or the end of the log.
          raftLog.lastCommittedEntryNum = commitUpTo(lastCommittedEntryNum, request.lastCommittedEntryNumber)
        }

        // OK response
        raftLog.lastCommittedEntryNum.let {
          replicaAppendLogResponse {
            this.status = KvasProto.ReplicaAppendLogResponse.Status.OK
            this.termNumber = this@KvasRaftNode.currentTerm
            this.lastCommittedEntry = raftLog.storage.createView().run {
              positionAt(it)
              get()
            } ?: LogEntry.getDefaultInstance()
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

  private fun commitUpTo(firstEntry: LogEntryNumber, lastEntry: LogEntryNumber): LogEntryNumber {
    val log = LoggerFactory.getLogger("Follower.CommitLog")
    var lastCommitted = firstEntry
    val commitView = raftLog.storage.createView()

    commitView.positionAt(firstEntry)
    commitView.forward()
    while (true) {
      val isBreak = commitView.get()?.let {
        if (it.entryNumber.compareTo(lastEntry) > 0) {
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

  override fun validateToken(requestToken: Int) = true
}

/**
 * This class is responsible for the aspects of replicating log entries to the follower nodes.
 *
 */
data class ReplicationController(
  val replica2logSender: MutableMap<String, LogSender> = mutableMapOf(),
  private val replicationOutChannel: Channel<LogEntryNumber> = Channel(),
  private val replicationCounter: MutableMap<LogEntryNumber, Int> = mutableMapOf(),
  private val replicationLock: ReentrantLock = ReentrantLock(),
  private val replicationTrigger: Condition = replicationLock.newCondition(),
  private val quorumSize: Int,
  private val logSenderFactory: (outChannel: Channel<LogEntryNumber>, address: String)->RaftLogSender
) {
  private var heartbeatTimeout = Timer("Heartbeat Timeout")
  fun addReplica(replicaAddress: String): KvasProto.ReplicaJoinGroupResponse {
    synchronized(replica2logSender) {
      val replicaToken = replica2logSender.size + 1
      val logSender = logSenderFactory(replicationOutChannel, replicaAddress)
      replica2logSender[replicaAddress] = logSender
      return replicaJoinGroupResponse { this.replicaToken = replicaToken }.also {
        LoggerFactory.getLogger("Primary.RegisterReplica").debug("Registered replica {}", replicaAddress)
      }
    }
  }

  fun start(allNodes: List<String>) {
    synchronized(replica2logSender) {
      allNodes.forEach { address ->
        val logSender = logSenderFactory(replicationOutChannel, address)
        replica2logSender[address] = logSender
      }
    }
    heartbeatTimeout.scheduleAtFixedRate(0, HEARTBEAT_PERIOD) {
      replicate()
    }
  }

  fun stop() {
    println("stopping replication")
    heartbeatTimeout.cancel()
  }

  // TODO: you need to modify this code as appropriate for the Raft log replication
  // Code written here is a prototype and its purpose is to provide a sketch of the log replication.
  // IT IS NOT A VALID IMPLEMENTATION OF RAFT LOG REPLICATION.
  suspend fun receiveReplicationResults(onSuccessfulReplication: (LogEntryNumber)->Unit) {
    // We take the results of replication of every log entry from the channel. Once a new result arrives, we update
    // the accumulated result and notify all possibly waiting requests.
    for (result in replicationOutChannel) {
      replicationLock.withLock {
        val counter = 1 + (replicationCounter[result] ?: 0)
        replicationCounter[result] = counter
        replicationTrigger.signalAll()

        // We update the last committed entry number here rather than in putValue method because it is possible that
        // the entry becomes committed when its corresponding Put request has already timed out, or even when the leader
        // restarts.
        if (counter >= quorumSize) {
          onSuccessfulReplication(result)
        }
      }
    }
  }

  fun replicateToQuorum(logEntry: LogEntryNumber): Result<StatusCode, Throwable> {
    replica2logSender.values.forEach { sender -> sender.run() }
    while (true) {
      replicationLock.withLock {
        // We are waiting for at least quorumSize messages indicating that logEntry was successfully replicated.
        if (replicationCounter.getOrDefault(logEntry, 0) >= quorumSize) {
          return Ok(StatusCode.OK)
        } else {
          replicationTrigger.await()
        }
      }
    }
  }

  internal fun replicate() {
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
  private val replicaAddress: String
) : LogSender(0) {

  private val log = LoggerFactory.getLogger("Primary.AppendLog")
  private val runLock = ReentrantLock()
  override fun run() {
    log.info("Running LogSender to {}", replicaAddress)
    replicationScope.launch {
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
          KvasProto.ReplicaAppendLogResponse.Status.PREV_MISMATCH -> {
            // For the purposes of this prototype, we break if we have a log mismatch.
            // TODO: you need to find out how to handle it
            break
          }
          KvasProto.ReplicaAppendLogResponse.Status.REJECT -> {
            if (resp.termNumber > server.currentTerm) {
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
  }
}

class RaftLogView(private val entries: List<LogEntry>, private var pos: Int = 0) {
  fun get(): LogEntry? = if (entries.size > pos) entries[pos] else null

  fun forward(): Boolean {
    println("LogView.frward: entries size=${entries.size} pos=$pos")
    if (pos < entries.size) pos++
    return pos < entries.size
  }

  fun positionAt(entry: LogEntryNumber): Boolean {
    val idx = entries.indexOfLast {
      it.entryNumber == entry
    }
    return if (idx == -1) {
      false
    } else {
      pos = idx
      true
    }
  }
}


class LogStorage {
  private val entries = mutableListOf<LogEntry>()

  fun lastOrNull(): LogEntry? = entries.lastOrNull()

  fun add(entry: LogEntry) {
    synchronized(entries) {
      entries.add(entry)
      println("log entries size=${entries.size}")
    }
  }

  fun createView() = RaftLogView(entries)
}

private val replicationScope = CoroutineScope(Executors.newCachedThreadPool().asCoroutineDispatcher())
private const val HEARTBEAT_PERIOD = 2000L
