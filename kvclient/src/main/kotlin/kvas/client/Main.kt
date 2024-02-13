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
import kvas.proto.kvasGetRequest
import kvas.proto.kvasPutRequest

/**
 * Реализация клиента, общающаяся с GRPC сервером.
 */
class KvasClient(channel: ManagedChannel) {
  private val syncStub = KvasGrpc.newBlockingStub(channel)

  fun get(key: String): String? {
    val resp = syncStub.getValue(kvasGetRequest { this.key = key })
    return if (resp.hasValue()) resp.value.value else null
  }

  fun put(key: String, value: String) {
    syncStub.putValue(kvasPutRequest {
      this.key = key
      this.value = value
    })
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
