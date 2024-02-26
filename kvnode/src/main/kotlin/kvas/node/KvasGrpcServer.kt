// This file include a reference implementation of the KVAS node. You can copy this code if necessary, or may inherit
// from this code if it makes sense, but most likely your own KVAS node implementation will be considerably different.
//
// Sharding: this code implements linear hashing sharding method.
package kvas.node

import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kvas.proto.*
import kvas.proto.KvasProto.KeyValue
import kvas.proto.KvasProto.KvasGetRequest
import kvas.proto.KvasProto.KvasGetResponse
import kvas.proto.KvasProto.RegisterShardRequest
import kvas.proto.KvasProto.RegisterShardResponse.StatusCode
import kvas.proto.KvasProto.ShardInfo
import kvas.util.LinearHashing
import kvas.util.LinearHashing.shardNumber
import kvas.util.kvas
import kvas.util.toHostPort
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import kotlin.concurrent.timer


/**
 * This is base class for both master and node servers. It stores the data in-memory and
 * responds to get/putValue and moveData requests.
 *
 * There is a dependency on linear hashing in moveData implementation.
 */
open class KvasGrpcServer(protected val selfAddress: String) : KvasGrpcKt.KvasCoroutineImplBase() {
  // In-memory storage
  private val key2value = mutableMapOf<String, String>()

  // This shard token
  protected var token: Int? = null

  /**
   * getValue returns success if the token supplier in the request matches this node token, that means that
   * the client has up-to-date information about the shard distribution.
   *
   * Please refer to ShardInfo protocol buffer docs in kvas.proto file for the meaning of shard_token value.
   */
  override suspend fun getValue(request: KvasGetRequest): KvasGetResponse =
    token?.let {
      // If the token is assigned and it matches the token in the request, we serve it.
      if (it == request.shardToken) {
        kvasGetResponse {
          code = KvasGetResponse.StatusCode.OK
          key2value[request.key]?.let {
            this.value = StringValue.of(it)
          }
        }
      } else null
    } ?: run {
      kvasGetResponse { code = KvasGetResponse.StatusCode.REFRESH_SHARDS }
    }

  /**
   * putValue returns success if the token supplier in the request matches this node token, that means that
   * the client has up-to-date information about the shard distribution.
   *
   * Please refer to ShardInfo protocol buffer docs in kvas.proto file for the meaning of shard_token value.
   */
  override suspend fun putValue(request: KvasProto.KvasPutRequest): KvasProto.KvasPutResponse {
    val log = LoggerFactory.getLogger("Node.PutValue")
    log.debug("token={}, request=[{}]", token, request)
    val response = token?.let {
      // If the token is assigned and it matches the token in the request, we serve it.
      if (it == request.shardToken) {
        key2value[request.key] = request.value
        kvasPutResponse {
          code = KvasProto.KvasPutResponse.StatusCode.OK
        }
      } else null
    } ?: run {
      kvasPutResponse { code = KvasProto.KvasPutResponse.StatusCode.REFRESH_SHARDS }
    }
    return response
  }
  protected fun putValue(entry: KeyValue) {
    key2value[entry.key] = entry.value
  }

  /**
   * This function returns a stream of key-value pair that should be moved to the node where the request was issued from.
   * The keys to move are defined by the linear hashing sharding method.
   * The moved entries are removed from the in-memory storage immediately.
   */
  override fun moveData(request: KvasProto.MoveDataRequest): Flow<KvasProto.KeyValue> {
    val log = LoggerFactory.getLogger("Node.MoveData")
    log.debug("request={}", request)
    val entriesMoved = key2value.entries
      .filter { shardNumber(it.key, request.shardsCount) == request.destinationShardToken }
      .toList()
    log.debug("Will move these entries: {}", entriesMoved)
    return entriesMoved.map { KeyValue.newBuilder().setKey(it.key).setValue(it.value).build() }.asFlow().also {
      key2value.entries.removeAll(entriesMoved)
    }
  }
}


/**
 * This is the master node. It adds methods for registering and listing the shards.
 */
class KvasGrpcServerMaster(selfAddress: String) : KvasGrpcServer(selfAddress) {
  // Shard mapping, maps shard tokens to node addresses.
  private val token2node = mutableMapOf<Int, String>()

  init {
    // Master token is 0.
    token = 0
  }
  override suspend fun registerShard(request: KvasProto.RegisterShardRequest): KvasProto.RegisterShardResponse {
    val log = LoggerFactory.getLogger("Master.RegisterShard")
    log.debug("request={}", request)
    if (request.hasShardToken()) {
      // Node re-joins the cluster
      return token2node[request.shardToken.value]?.let {
        if (it == request.nodeAddress) {
          log.debug("Node={} confirmed its membership", request.nodeAddress)
          registerShardResponse {
            code = StatusCode.OK
            shardToken = request.shardToken.value
          }
        } else {
          log.debug("It appears that there is already a node={} with token={}", it, request.shardToken.value)
          registerShardResponse { code = StatusCode.TOKEN_CONFLICT }
        }
      } ?: run {
        log.debug("Node={} and token={} registered in the cluster", request.nodeAddress, request.shardToken.value)
        token2node[request.shardToken.value] = request.nodeAddress
        registerShardResponse {
          code = StatusCode.OK
          shardToken = request.shardToken.value
        }
      }
    } else {
      return if (token2node.filterValues { it == request.nodeAddress }.isNotEmpty()) {
        log.debug("Node={} is alredy registered with another token", request.nodeAddress)
        registerShardResponse { code = StatusCode.OTHER }
      } else {
        // Node is new, and we need to assign a token according to our sharding function.
        assignToken(request).let {
          log.debug("Node={} was assigned a token={}", request.nodeAddress, it)
          token2node[it] = request.nodeAddress
          log.debug("All registered shards now: {}", token2node)
          registerShardResponse {
            code = StatusCode.OK
            shardToken = it
            shards.addAll(allShards())
          }
        }
      }
    }
  }

  protected fun assignToken(request: RegisterShardRequest): Int = token2node.size + 1

  override suspend fun getShards(request: KvasProto.GetShardsRequest): KvasProto.GetShardsResponse = getShardsResponse {
    this.shards.addAll(allShards())
  }

  private fun allShards() = mutableListOf<ShardInfo>().also {
    it.addAll(token2node.map { ShardInfo.newBuilder().setNodeAddress(it.value).setShardToken(it.key).build() })
    it.add(ShardInfo.newBuilder().setNodeAddress(selfAddress).setShardToken(0).build())
  }
}

/**
 * This is the worked node implementation. It continuously registers itself at the master node.
 * When it registers the first time, it receives a shard token and copies some data from the shard, defined by
 * the linear sharding function.
 */
class KvasGrpcServerNode(selfAddress: String, private val masterAddress: String) : KvasGrpcServer(selfAddress) {
  private val syncStub = masterAddress.toHostPort().let {
    kvas(it.first, it.second)
  }

  init {
    timer(name = "Register node", period = 10000) {
      registerItself()
    }
  }

  private fun registerItself() {
    val log = LoggerFactory.getLogger("Node.RegisterItself")
    // Send a registerShard request, adding the assigned token if it is available.
    val response = syncStub.registerShard(registerShardRequest {
      this@KvasGrpcServerNode.token?.let {
        this.shardToken = Int32Value.of(it)
      } ?: run {
        this.clearShardToken()
      }
      this.nodeAddress = selfAddress
    })
    when (response.code) {
      StatusCode.OK -> completeRegistration(response).also {
        log.debug("Registered with token={}", response.shardToken)
      }
      else -> {
        log.error("Can't register at master, received {}", response)
        System.exit(1)
      }
    }
  }

  /**
   */
  protected fun completeRegistration(response: KvasProto.RegisterShardResponse) {
    if (this.token == null) {
      this.token = response.shardToken

      // If we have no token, we need to copy data from the shard where it is located now.
      response.shardsList.find { it.shardToken == getSplitShardNumber() }?.let {
        moveData(it.nodeAddress, response.shardsList)
      }
    } else {
      this.token = response.shardToken
    }
  }

  private fun moveData(nodeAddress: String, shards: List<ShardInfo>) {
    val log = LoggerFactory.getLogger("Node.RegisterItself")
    log.debug("Will try to copy data from node={}, with token={}", nodeAddress, getSplitShardNumber())
    nodeAddress.toHostPort().let { hostPort ->
      kvas(hostPort.first, hostPort.second).moveData(moveDataRequest {
        this.destinationShardToken = this@KvasGrpcServerNode.token!!
        this.shards.addAll(shards)
      }).forEach { putValue(it) }
    }
  }

  override suspend fun registerShard(request: RegisterShardRequest) = throw IllegalStateException("I am not a master")

  override suspend fun getShards(request: KvasProto.GetShardsRequest) = throw IllegalStateException("I am not a master")

  /**
   * Returns the shard number where this shard shall copy data from, using linear hashing method.
   */
  private fun getSplitShardNumber() = LinearHashing.splitShardNumber(this.token!!)
}