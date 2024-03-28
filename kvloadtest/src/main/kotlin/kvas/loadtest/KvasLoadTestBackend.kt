package kvas.loadtest

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kvas.proto.*
import kvas.proto.KvasGrpc.KvasBlockingStub
import kvas.util.LinearHashing
import kvas.util.toHostPort
import kotlin.system.exitProcess

/**
 * Implementation of the load test backend that talks to KVAS servers.
 */
class KvasLoadTestBackend(
  private val shardNumberPut: (String)->Int,
  private val shardNumberGet: (String)->Int = shardNumberPut,
  private val address2shardNumber: (String)->Int?,
  private val shardStubFactory: (Int)->KvasGrpc.KvasBlockingStub?,
  private val replaceShard: (Int)->Int,
  private val delayMs: Long = 0) : Backend {

  private val shard2stub = mutableMapOf<Int, KvasGrpc.KvasBlockingStub>()
  private val shard2requestCount = mutableMapOf<Int, Int>()
  private var leaderShardNumber: Int = -1
  private lateinit var leaderStub: KvasBlockingStub

  override val stats: String get() {
    return shard2requestCount.map { entry -> "Sent ${entry.value} requests to shard#${entry.key}" }.joinToString(separator = ", ")
  }
  override fun put(key: String, value: String) {
    if (delayMs > 0) {
      runBlocking { delay(delayMs) }
    }
    if (leaderShardNumber == -1) {
      leaderShardNumber = shardNumberPut(key)
      leaderStub = shard2stub.getOrPut(leaderShardNumber) {
        shardStubFactory(leaderShardNumber) ?: error("Can't create stub for the shard $leaderShardNumber")
      }
    }
    var failureCounter = 0
    while(true) {
      try {
        shard2requestCount.increment(leaderShardNumber)
        val response = leaderStub.putValue(kvasPutRequest {
          this.key = key
          this.value = value
          this.shardToken = leaderShardNumber
        })
        when (response.code) {
          KvasProto.KvasPutResponse.StatusCode.REDIRECT -> {
            leaderStub = address2shardNumber(response.leaderAddress)?.let {
              leaderShardNumber = it
              shard2stub.getOrPut(it) {
                shardStubFactory(leaderShardNumber) ?: error("Can't create stub for the shard $leaderShardNumber")
              }
            } ?: error("Can't map address ${response.leaderAddress} to a node number")
            println("Redirecting request to ${response.leaderAddress}, node#$leaderShardNumber")
          }
          KvasProto.KvasPutResponse.StatusCode.OK -> {
            // nice!
            break
          }
          else -> {
            error("Response code for key $key is not OK: $response. Shard token=$leaderShardNumber")
            exitProcess(1)
          }
        }
      } catch (ex: Exception) {
        failureCounter++
        println("Failed to PUT key=$key into a shard=$leaderShardNumber")
        leaderShardNumber = replaceShard(leaderShardNumber)
        println("Replaced with shard $leaderShardNumber")
        leaderStub = shard2stub.getOrPut(leaderShardNumber) {
          shardStubFactory(leaderShardNumber) ?: error("Can't create stub for the shard $leaderShardNumber")
        }
        if (failureCounter > 10) {
          throw RuntimeException(ex)
        } else {
          continue
        }
      }
    }
  }

  override fun get(key: String): String? {
    val shardNumber = shardNumberGet(key)
    val stub = shard2stub.getOrPut(shardNumber) {
      shardStubFactory(shardNumber) ?: error("Can't create stub for the shard $shardNumber")
    }
    shard2requestCount.increment(shardNumber)
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

  protected val shardTokens: List<Int> get() = shardToken2stub.keys.sorted()
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

  private fun newStub(address: String) = address.toHostPort().let {
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

fun <T> MutableMap<T, Int>.increment(key: T) {
  this[key] = this.getOrDefault(key, 0) + 1
}
