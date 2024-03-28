package kvas.loadtest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.runBlocking
import kvas.proto.kvasOfflineRequest
import kvas.util.kvas
import kvas.util.toHostPort
import kotlin.time.ExperimentalTime

class Main: CliktCommand() {
  override fun run() {
  }
}

class Sharding: CliktCommand("Runs the load test on sharded database") {
  val httpPort by option().int().default(0)
  val kvasAddress by option().default("127.0.0.1:9000")
  val keyCount by option().int().default(10)
  val connectionCount by option().int().default(1)
  val method by option().choice("linear", "simple", "consistent").default("linear")
  val workload by option(help="What type of the workload shall be generated").enum<Workload>().default(Workload.MIXED)
  init {
    context {
      helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
    }
  }
  override fun run() {
    if (httpPort != 0) {
      embeddedServer(Netty, port = httpPort) {
        module()
      }.start(wait = true)
    } else {
      runBlocking {
        val result = runShardingLoadTest(kvasAddress, workload, connectionCount, keyCount, method)
        println("""
          |------------------------------------------------------------
          |$result
        """.trimMargin())
      }
      System.exit(0)
    }
  }
}

class Replication: CliktCommand("Runs the load test against the replicate database") {
  val httpPort by option(help="Start an HTTP server on this port (the remaining arguments are ignored)").int().default(0)
  val kvasAddress by option(help="Address of the primary node of the replication group").default("127.0.0.1:9000")
  val keyCount by option(help="How many keys shall be generated").int().default(10)
  val connectionCount by option(help="How many connections shall be used for every node").int().default(1)
  val workload by option(help="What type of the workload shall be generated").enum<Workload>().default(Workload.MIXED)
  val leaderless by option(help="Is it a leader-based replication").flag()
  val delayMs by option(help="Pause between the requests").int().default(0)
  init {
    context {
      helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
    }
  }

  override fun run() {
    if (httpPort != 0) {
      embeddedServer(Netty, port = httpPort) {
        module()
      }.start(wait = true)
    } else {
      runBlocking {
        val result = runReplicationLoadTest(kvasAddress, leaderless, workload, connectionCount, keyCount, delayMs.toLong())
        println("""
          |------------------------------------------------------------
          |$result
        """.trimMargin())
      }
      System.exit(0)
    }
  }
}

class SetAvailable: CliktCommand("Sets a node offline or online") {
  val node by option(help="Node address in IP:port format")
  val available by option(help="Is this nove available or not").choice("yes", "no")

  override fun run() {
    node?.toHostPort()?.let { kvas(it.first, it.second).setOffline(kvasOfflineRequest {
      isOffline = "no".equals(available, ignoreCase = true)
    }) }
  }
}
fun main(args: Array<String>): Unit {
  Main().subcommands(Sharding(), Replication(), SetAvailable()).main(args)
}

//fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

fun Application.module() {
  configureRouting()
  configureSerialization()
}

fun Application.configureRouting() {
  routing {
    route("/") {
      get("{master?}{connections?}{keys?}{method?}{?leaderless}") {
        val masterIP = call.parameters["master"]
          ?: return@get call.respondText("Missing master IP", status = HttpStatusCode.BadRequest )
        val connectionCount = call.parameters["connections"]?.toInt()
          ?: 4
        val keyCount = call.parameters["keys"]?.toInt()
          ?: 100
        val method = call.parameters["method"] ?: "linear"
        val workload = call.parameters["workload"]?.let { Workload.valueOf(it.uppercase()) } ?: Workload.MIXED
        val results = runShardingLoadTest(masterIP, workload, connectionCount, keyCount, method)
        call.respondText("""
          |Load test against $masterIP using $connectionCount connections and $keyCount keys
          |----------------------------------------------------------------
          |$results
        """.trimMargin())
      }
    }
  }
}

fun Application.configureSerialization() {
  install(ContentNegotiation) {
    json()
  }
}

@OptIn(ExperimentalTime::class)
suspend fun runShardingLoadTest(masterAddress: String, workload: Workload, connectionCount: Int, keyCount: Int, method: String): String {
  println("running load test with master=${masterAddress} connection count=${connectionCount} and key count=${keyCount}")
  val sharding = when (method) {
    "simple" -> SimpleHashingRouter(masterAddress)
    "consistent" -> ConsistentHashingRouter(masterAddress)
    "linear" -> LinearHashingRouter(masterAddress)
    else -> return "Unknown sharding method $method"
  }
  return runTest(workload, keyCount, connectionCount) { backendNum ->
    KvasLoadTestBackend(shardNumberPut = sharding::shardNumber,
      address2shardNumber = { TODO("Not yet implemented") },
      replaceShard = {it},
      shardStubFactory = sharding::getStub)
  }
}

@OptIn(ExperimentalTime::class)
suspend fun runReplicationLoadTest(masterAddress: String, leaderless: Boolean,  workload: Workload, connectionCount: Int, keyCount: Int, delayMs: Long): String {
  println("running ${if (leaderless) "leaderless" else "leader-based"} load test with master=${masterAddress} connection count=${connectionCount} and key count=${keyCount}")
  val replicaRouter = ReplicaRouter(masterAddress)
  return runTest(workload, keyCount, connectionCount) {replicaNum ->
    KvasLoadTestBackend(
      shardNumberPut = if (leaderless) replicaRouter::replicaRandomNumber else replicaRouter::replicaLeaderNumber,
      shardNumberGet = replicaRouter::replicaRandomNumber,
      address2shardNumber = replicaRouter::address2shardNumber,
      shardStubFactory = replicaRouter::getStub,
      delayMs = delayMs,
      replaceShard = replicaRouter::replaceShard
    )
  }
}