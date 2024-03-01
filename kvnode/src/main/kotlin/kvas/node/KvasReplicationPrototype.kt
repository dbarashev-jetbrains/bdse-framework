package kvas.node

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kvas.proto.*
import kvas.proto.KvasProto.*
import kvas.util.kvas
import kvas.util.toHostPort
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors

class KvasReplicationLeader(selfAddress: String) : KvasGrpcServer(selfAddress) {
  private val senderScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
  private val logEntries = ConcurrentLinkedQueue<LogEntry>()
  private val replica2logSender = mutableMapOf<String, LogSender>()

  /**
   * We apply the value as usual, write it to the log and synchronously replicate.
   * This protocol is flawed in some aspects, e.g. it is possible that write is committed on the leader only,
   * without even being written to the log.
   */
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    return try {
      super.putValue(request).also {
        appendLog(request)
        replicate()
      }
    } catch (ex: Exception) {
      LoggerFactory.getLogger("Node.PutValue").error("Failure when applying write request {}", request, ex)
      throw ex
    }
  }

  override fun validateToken(requestToken: Int) = true

  /**
   * Processes the request to join a new replica to this replication group and initiates replicating an existing log.
   */
  override suspend fun replicaJoinGroup(request: KvasProto.ReplicaJoinGroupRequest): KvasProto.ReplicaJoinGroupResponse {
    synchronized(replica2logSender) {
      val replicaToken = replica2logSender.size + 1
      val logSender = LogSender(logEntries, request.replicaAddress, replicaToken,
        if (request.hasLastCommittedEntry()) request.lastCommittedEntry.ordinalNumber else -1,
        onReplicaFail = this::removeReplica
      )
      replica2logSender[request.replicaAddress] = logSender
      return replicaJoinGroupResponse { this.replicaToken = replicaToken }.also {
        LoggerFactory.getLogger("Primary.RegisterReplica").debug("Registered replica {}", request.replicaAddress)
        senderScope.launch {
          // We need to replicate the existing log, however, it makes sense to wait a bit to let the replica receive
          // the response to JoinGroup request.
          delay(1000)
          logSender.run()
        }
      }
    }
  }

  /**
   * Returns the list of all replicas, including the leader.
   * By convention, leader's token is 0, this distinguishes the writeable leader from the read-only followers.
   */
  override suspend fun replicaGetGroup(request: KvasProto.ReplicaGetGroupRequest): KvasProto.ReplicaGetGroupResponse {
    return replicaGetGroupResponse {
      this.replicas.addAll(replica2logSender.entries.map { entry ->
        ShardInfo.newBuilder().setNodeAddress(entry.key).setShardToken(entry.value.replicaToken).build()
      })
      this.replicas.add(ShardInfo.newBuilder().setNodeAddress(selfAddress).setShardToken(0).build())
    }
  }

  private fun appendLog(putRequest: KvasPutRequest): LogEntry {
    return synchronized(logEntries) {
      LogEntry.newBuilder()
        .setKv(KeyValue.newBuilder().setKey(putRequest.key).setValue(putRequest.value).build())
        .setOrdinalNumber((logEntries.lastOrNull()?.ordinalNumber ?: 0) + 1)
        .build().also {
          logEntries.add(it)
        }
    }
  }

  private fun replicate() {
    replica2logSender.values.forEach { it.run() }
  }

  private fun removeReplica(replicaAddress: String) {
    synchronized(replica2logSender) {
      replica2logSender.remove(replicaAddress)
    }
  }
}

/**
 * This is a follower node. It receives the replication log and applies it locally. It keeps the number of the last
 * applied log entry.
 */
class KvasReplicationFollower(selfAddress: String, private val primaryAddress: String) : KvasGrpcServer(selfAddress) {
  private var replica_token = -1
  private var getSuccessCounter = 0
  private val primaryStub = primaryAddress.toHostPort().let { kvas(it.first, it.second) }
  init {
    val response = primaryStub.replicaJoinGroup(replicaJoinGroupRequest {
      this.replicaAddress = selfAddress
      this.clearLastCommittedEntry()
    })
    replica_token = response.replicaToken
  }

  /**
   * Process getValue as usual, just count the successful requests.
   */
  override suspend fun getValue(request: KvasProto.KvasGetRequest): KvasProto.KvasGetResponse {
    return super.getValue(request).also {
      if (it.code == KvasProto.KvasGetResponse.StatusCode.OK && it.value.isInitialized) {
        getSuccessCounter++
      }
    }
  }

  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    error("I am read-only, bro")
  }

  /**
   * We don't care about the tokens in the requests from client.
   */
  override fun validateToken(requestToken: Int) = true

  override suspend fun replicaAppendLog(request: KvasProto.ReplicaAppendLogRequest): KvasProto.ReplicaAppendLogResponse {
    var lastCommittedEntry: LogEntry? = null
    request.entriesList.forEach { entry ->
      super.putValue(kvasPutRequest {
        this.key = entry.kv.key
        this.value = entry.kv.value
        this.shardToken = replica_token
      })
      lastCommittedEntry = entry
    }
    LoggerFactory.getLogger("Follower.AppendLog").debug(
      "Appended {} log records. Now we have {} keys and last committed is {}",
      request.entriesList.size,
      this.keyCount,
      lastCommittedEntry
    )
    return lastCommittedEntry?.let {
      replicaAppendLogResponse {
        this.lastCommittedEntry = it
      }
    } ?: replicaAppendLogResponse {  }
  }
}

/**
 * This class keeps the state of the log replication for a single replica and sends out AppendLog requests.
 * Communication failures are reported to the server (and replica is removed from the list).
 */
class LogSender(private val entries: Queue<LogEntry>,
                private val replicaAddress: String,
                internal val replicaToken: Int,
                private var lastCommittedEntry: Int = -1,
                private val onReplicaFail: (String)->Unit
  ) {
  private val stub = replicaAddress.toHostPort().let { kvas(it.first, it.second) }
  private val log = LoggerFactory.getLogger("Primary.AppendLog")
  fun run() {
    val entries = entries.dropWhile { it.ordinalNumber <= lastCommittedEntry }.sortedBy { it.ordinalNumber }
    if (entries.isEmpty()) {
      return
    }
    log.debug("Sending {} log records [{}..{}] to {}",
      entries.size, entries.first().ordinalNumber, entries.last().ordinalNumber, replicaAddress
    )
    try {
      val response = stub.replicaAppendLog(replicaAppendLogRequest {
        this.entries.addAll(entries)
      })
      if (entries.find { it.ordinalNumber == response.lastCommittedEntry.ordinalNumber } == null) {
        throw RuntimeException("Can't find entry with the ordinal number ${response.lastCommittedEntry.ordinalNumber}")
      }
      lastCommittedEntry = response.lastCommittedEntry.ordinalNumber
    } catch (ex: Exception) {
      log.error("Failed to send AppendLog request to {}", replicaAddress, ex)
      onReplicaFail(replicaAddress)
    }
  }
}