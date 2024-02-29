import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kvas.node.KvasGrpcServer
import kvas.proto.*
import kvas.proto.KvasProto.KeyValue
import kvas.proto.KvasProto.KvasPutRequest
import kvas.proto.KvasProto.LogEntry
import kvas.proto.KvasProto.ShardInfo
import kvas.util.kvas
import kvas.util.toHostPort
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors

class KvasReplicationLeader(selfAddress: String) : KvasGrpcServer(selfAddress) {
  private val senderScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
  private val logEntries = mutableListOf<LogEntry>()
  private val replica2logSender = mutableMapOf<String, LogSender>()
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    return try {
      super.putValue(request).also {
        appendLog(request)
        replicate()
      }
    } catch (ex: Exception) {
      ex.printStackTrace()
      throw ex
    }
  }

  override fun validateToken(requestToken: Int) = true

  override suspend fun replicaJoinGroup(request: KvasProto.ReplicaJoinGroupRequest): KvasProto.ReplicaJoinGroupResponse {
    synchronized(replica2logSender) {
      val logSender = LogSender(logEntries, request.replicaAddress,
        if (request.hasLastCommittedEntry()) request.lastCommittedEntry.ordinalNumber else -1
      )
      replica2logSender[request.replicaAddress] = logSender
      return replicaJoinGroupResponse { this.replicaToken = replica2logSender.size }.also {
        LoggerFactory.getLogger("Primary.RegisterReplica").debug("Registered replica {}", request.replicaAddress)
        senderScope.launch {
          delay(1000)
          logSender.run()
        }
      }
    }
  }

  override suspend fun replicaGetGroup(request: KvasProto.ReplicaGetGroupRequest): KvasProto.ReplicaGetGroupResponse {
    return replicaGetGroupResponse {
      this.replicas.addAll(replica2logSender.keys.mapIndexed { idx, address ->
        ShardInfo.newBuilder().setNodeAddress(address).setShardToken(idx + 1).build()
      })
      this.replicas.add(ShardInfo.newBuilder().setNodeAddress(selfAddress).setShardToken(0).build())
    }
  }

  private fun appendLog(putRequest: KvasPutRequest) {
    synchronized(logEntries) {
      logEntries.add(
        LogEntry.newBuilder()
          .setKv(KeyValue.newBuilder().setKey(putRequest.key).setValue(putRequest.value).build())
          .setOrdinalNumber((logEntries.lastOrNull()?.ordinalNumber ?: 0) + 1)
          .build()
      )
    }
  }

  private fun replicate() {
    replica2logSender.values.forEach { it.run() }
  }
}

class KvasReplicationFollower(selfAddress: String, private val primaryAddress: String) : KvasGrpcServer(selfAddress) {
  private var replica_token = -1
  private val primaryStub = primaryAddress.toHostPort().let { kvas(it.first, it.second) }
  init {
    val response = primaryStub.replicaJoinGroup(replicaJoinGroupRequest {
      this.replicaAddress = selfAddress
      this.clearLastCommittedEntry()
    })
    replica_token = response.replicaToken
  }
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    error("I am read-only, bro")
  }

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

class LogSender(private val entries: List<LogEntry>,
                private val replicaAddress: String,
                private var lastCommittedEntry: Int = -1) {
  private val stub = replicaAddress.toHostPort().let { kvas(it.first, it.second) }

  fun run() {
    val entries = entries.dropWhile { it.ordinalNumber <= lastCommittedEntry }.sortedBy { it.ordinalNumber }
    if (entries.isEmpty()) {
      return
    }
    LoggerFactory.getLogger("Primary.AppendLog").debug("Sending {} log records [{}..{}] to {}",
      entries.size, entries.first().ordinalNumber, entries.last().ordinalNumber, replicaAddress
    )
    val response = stub.replicaAppendLog(replicaAppendLogRequest {
      this.entries.addAll(entries)
    })
    if (entries.find { it.ordinalNumber == response.lastCommittedEntry.ordinalNumber} == null) {
      throw RuntimeException("Can't find entry with the ordinal number ${response.lastCommittedEntry.ordinalNumber}")
    }
    lastCommittedEntry = response.lastCommittedEntry.ordinalNumber
  }


}