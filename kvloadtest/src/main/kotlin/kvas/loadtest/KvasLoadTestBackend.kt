package kvas.loadtest

import io.grpc.ManagedChannelBuilder
import kvas.proto.*
import kvas.util.LinearHashing
import kvas.util.toHostPort
import kotlin.system.exitProcess

/**
 * Implementation of the load test backend that talks to KVAS servers.
 */
class KvasLoadTestBackend(
  private val shardNumberPut: (String)->Int,
  private val shardNumberGet: (String)->Int = shardNumberPut,
  private val shardStubFactory: (Int)->KvasGrpc.KvasBlockingStub?) : Backend {

  private val shard2stub = mutableMapOf<Int, KvasGrpc.KvasBlockingStub>()
  override fun put(key: String, value: String) {
    val shardNumber = shardNumberPut(key)
    val stub = shard2stub.getOrPut(shardNumber) {
      shardStubFactory(shardNumber) ?: error("Can't create stub for the shard $shardNumber")
    }

    try {
      val response = stub.putValue(kvasPutRequest {
        this.key = key
        this.value = value
        this.shardToken = shardNumber
      })
      if (response.code != KvasProto.KvasPutResponse.StatusCode.OK) {
        error("Response code for key $key is not OK: $response. Shard token=$shardNumber")
        exitProcess(1)
      }
    } catch (ex: Exception) {
      println("Failed to PUT key=$key")
      ex.printStackTrace()
      throw RuntimeException(ex)
    }
  }

  override fun get(key: String): String? {
    val shardNumber = shardNumberGet(key)
    val stub = shard2stub.getOrPut(shardNumber) {
      shardStubFactory(shardNumber) ?: error("Can't create stub for the shard $shardNumber")
    }
    return stub.getValue(kvasGetRequest {
      this.key = key
      this.shardToken = shardNumber
    }).let {
      when (it.code) {
        KvasProto.KvasGetResponse.StatusCode.OK -> it.value.value
        else -> error("GET: response code is ${it.code}")
      }
    }
  }
}

/**
 * Client-side KVAS request router. It dispatches requests to one of the KVAS cluster nodes depending on the chosen sharding
 * method.
 */
abstract class ShardRouter(masterAddress: String) {
  private val shardToken2stub = mutableMapOf<Int, KvasGrpc.KvasBlockingStub>()

  protected val shardTokens: List<Int> = shardToken2stub.keys.sorted()
  protected val shardCount get() = shardToken2stub.size
  init {
    newStub(masterAddress).getShards(getShardsRequest {  }).also {
      println("shards: $it")
      it.shardsList.forEach {shardInfo ->
        shardToken2stub[shardInfo.shardToken] = newStub(shardInfo.nodeAddress)
        println("registered shard=${shardInfo.nodeAddress} with token=${shardInfo.shardToken}")
      }
    }

  }

  fun getStub(shardToken: Int) = shardToken2stub[shardToken]

  protected open fun newStub(address: String) = address.toHostPort().let {
    KvasGrpc.newBlockingStub(ManagedChannelBuilder.forAddress(it.first, it.second).usePlaintext().build())
  }

  /**
   * Implement this function depending on the sharding method.
   */
  abstract fun shardNumber(key: String): Int
}

/**
 * This router uses simple hash sharding. It calculates the remainder of the hash code divided by the total number
 * of shards.
 */
class SimpleHashingRouter(masterAddress: String) : ShardRouter(masterAddress) {
  override fun shardNumber(key: String) =
     (kvas.util.hashCode(key) % shardCount.toUInt()).toInt()
}

/**
 * This router uses linear hashing to calculate the target shard number.
 */
class LinearHashingRouter(masterAddress: String) : ShardRouter(masterAddress) {
  override fun shardNumber(key: String): Int = LinearHashing.shardNumber(key, shardCount)
}

/**
 * This router uses a consistent hash ring and finds the lower bound of tokens greater than a key hash code.
 */
class ConsistentHashingRouter(masterAddress: String) : ShardRouter(masterAddress) {
  override fun shardNumber(key: String): Int {
    val hashCode = kvas.util.hashCode(key)
    val sortedTokens = this.shardTokens
    return sortedTokens.firstOrNull { it.toUInt() >= hashCode } ?: sortedTokens[0]
  }
}

