package kvas.client

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.findOrSetObject
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kvas.proto.KvasGrpc
import kvas.proto.KvasGrpc.KvasFutureStub
import kvas.proto.KvasProto.ShardInfo
import kvas.proto.getShardsRequest
import kvas.proto.kvasGetRequest
import kvas.proto.kvasPutRequest
import kvas.util.*
import kvas.util.LinearHashing.shardNumber
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.util.concurrent.ExecutionException

/**
 * Реализация клиента, общающаяся с GRPC сервером.
 */
class KvasClient(channel: ManagedChannel) {
  private val masterStub = KvasGrpc.newBlockingStub(channel)
  private val nodeStubs = mutableMapOf<String, KvasFutureStub>()
  private val log = LoggerFactory.getLogger("Client")

  fun getNodeForKey(key: String): ShardInfo {
    val shardList = masterStub.getShards(getShardsRequest {  }).shardsList
    val shardNumber = shardNumber(key, shardList.size)
    return shardList.find { it.shardToken == shardNumber }?.nodeAddress?.let {
      ShardInfo.newBuilder().setNodeAddress(it).setShardToken(shardNumber).build()
    } ?: throw RuntimeException("Can't find a shard with number $shardNumber")
  }

  fun getNodeStub(address: String) = nodeStubs.getOrPut(address) {
    kvasync(address.toHostPort().first, address.toHostPort().second)
  }

  fun get(key: String): String? {
    getNodeForKey(key).let {shard ->
      val resp = getNodeStub(shard.nodeAddress).getValue(kvasGetRequest {
        this.key = key
        this.shardToken = shard.shardToken
      }).get()
      return if (resp.hasValue()) resp.value.value else null
    }
  }

  fun put(key: String, value: String) {
    getNodeForKey(key).let {shard ->
      try {
        getNodeStub(shard.nodeAddress).putValue(kvasPutRequest {
          this.key = key
          this.shardToken = shard.shardToken
          this.value = value
        }).get()
      } catch (ex: ExecutionException) {
        log.error("Failed to put key={} on shard={}", key, shard)
        ex.printStackTrace()
      }
    }
  }
}

/**
 * Точка входа для квас-клиента. Разбирает аргументы командной строки и вызывает соответствующую команду.
 */
class Main : CliktCommand() {
  val kvasHost: String by option(help = "Kvas node host name").default("localhost")
  val kvasPort: Int by option(help = "Kvas node port number").int().default(9000)
  val kvasClient by findOrSetObject {
    KvasClient(ManagedChannelBuilder
      .forAddress(kvasHost, kvasPort)
      .usePlaintext().build()
    )
  }
  override fun run() {
    kvasClient.hashCode()
  }
}

/**
 * Команда get. Её аргументом является ключ, значение которого требуется найти в квасе.
 */
class Get : CliktCommand(name = "get") {
  val key by argument()
  val kvasClient by requireObject<KvasClient>()

  override fun run() {
    println(kvasClient.get(key))
  }
}

class Put : CliktCommand(name = "put") {
  val key by argument()
  val value by argument()
  val kvasClient by requireObject<KvasClient>()
  override fun run() {
    kvasClient.put(key, value)
    println("Kvas::putValue completed")
  }

}
fun main(args: Array<String>) = Main().subcommands(Get(), Put()).main(args)
