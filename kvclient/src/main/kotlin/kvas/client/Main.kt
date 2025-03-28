package kvas.client

import com.github.ajalt.clikt.core.*
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.clikt.parameters.types.int
import io.grpc.ManagedChannelBuilder
import kvas.proto.*
import kvas.proto.MapperGrpc.MapperBlockingStub
import kvas.setup.AllShardings
import kvas.setup.NotImplementedSharding
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress

/**
 * Entry point for the command-line application that interacts with a sharded key-value store using gRPC.
 */
class Main : CliktCommand() {
    // Command-line arguments
    val metadataAddress: String by option(help = "Metadata server address").default("localhost:9000")
    val shardingConfig by option("--sharding").choice(
        *AllShardings.ALL.keys.toTypedArray()
    ).default(AllShardings.NAIVE.first)

    // Client factory for the subcommands.
    val kvasClientFactory by findOrSetObject {
        this::createKvasClient
    }

    override fun run() {
        kvasClientFactory
    }

    private fun createKvasClient() =
        KvasClient(
            AllShardings.ALL[shardingConfig] ?: NotImplementedSharding,
            metadataAddress.toNodeAddress(),
            { nodeAddress ->
                MetadataServiceGrpc.newBlockingStub(
                    ManagedChannelBuilder.forAddress(nodeAddress.host, nodeAddress.port).usePlaintext().build()
                )
            },
            { nodeAddress ->
                DataServiceGrpc.newBlockingStub(
                    ManagedChannelBuilder.forAddress(nodeAddress.host, nodeAddress.port).usePlaintext().build()
                )
            },
            { nodeAddress ->
                StatisticsGrpc.newBlockingStub(
                    ManagedChannelBuilder.forAddress(nodeAddress.host, nodeAddress.port).usePlaintext().build()
                )
            },
            { nodeAddress ->
                OutageEmulatorServiceGrpc.newBlockingStub(
                    ManagedChannelBuilder.forAddress(nodeAddress.host, nodeAddress.port).usePlaintext().build())
            }
        )
}

/**
 * This command executes a single get request.
 */
class Get : CliktCommand(name = "get") {
    // Command-line argument
    val key by argument()

    val kvasClientFactory by requireObject<() -> KvasClient>()

    override fun run() {
        println(kvasClientFactory().get(key))
    }
}

/**
 * This command executes a single put request.
 */
class Put : CliktCommand(name = "put") {
    // Command line arguments
    val key by argument()
    val value by argument()

    val kvasClientFactory by requireObject<() -> KvasClient>()
    override fun run() {
        kvasClientFactory().put(key, value)
        println("Kvas::putValue completed")
    }
}

/**
 * Represents a shell interface for interacting with a KVAS instance.
 *
 * Users can execute the following:
 * - Typing a key (`<KEY>`) to perform a GET request and fetch the value associated with the key.
 * - Typing a key-value pair (`<KEY>=<VALUE>`) to execute a PUT request and store the value in the system.
 * - Pressing Enter or providing an empty line to exit the shell.
 */
class Shell : CliktCommand(name = "shell") {
    val writeNode by option().choice("leader", "random").default("leader")
    val kvasClientFactory by requireObject<() -> KvasClient>()
    override fun run() {
        println(
            """
      This is KVAS shell. 
      Type a plain key to execute GET request.
      Type <KEY>=<VALUE> to execute PUT request.
      Enter an empty line to exit.
      -------------------------------------------
      """.trimIndent()
        )
        val kvasClient = kvasClientFactory()
        while (true) {
            val input = readlnOrNull() ?: break
            if (input.isBlank()) {
                break
            }
            if (input.startsWith("/")) {
                processCommand(input, kvasClient)
            } else {
                val keyValue = input.split("=", limit = 2)
                if (keyValue.size == 2) {
                    kvasClient.put(keyValue[0], keyValue[1], writeNode.toWriteNodeSelector())
                } else {
                    val value = kvasClient.get(keyValue[0])
                    println("$keyValue=$value")
                }
            }
        }
    }

    private fun processCommand(input: String, kvasClient: KvasClient) {
        val words = input.split(" ")
        when (words[0]) {
            "/exit" -> System.exit(0)
            "/offline" -> {
                setAvailable(kvasClient, words.drop(1), false)
            }
            "/online" -> {
                setAvailable(kvasClient, words.drop(1), true)
            }
        }
    }

    private fun setAvailable(kvasClient: KvasClient, words: List<String>, isAvailable: Boolean) {
        if (words.isEmpty()) {
            println("Invalid number of arguments")
            return
        }
        words.forEach { word ->
            if (word.indexOf("..") < 0) {
                kvasClient.sendNodeAvailable(word, isAvailable)
            } else {
                val (src, dst) = word.split("..", limit = 2)
                kvasClient.sendLinkAvailable(src, dst, isAvailable)
                kvasClient.sendLinkAvailable(dst, src, isAvailable)
            }
        }

    }
}

/**
 * The `LoadTestCommand` class is a command-line tool used to perform load testing
 * on a backend system. It generates and executes a workload against the backend
 * to measure performance and consistency of read/write operations.
 */
class LoadTestCommand : CliktCommand(name = "loadtest") {
    val kvasClientFactory by requireObject<() -> KvasClient>()
    val keyCount by option().int().default(1)
    val clientCount by option().int().default(1)
    val workload by option().choice("READONLY", "MIXED").default("MIXED")
    val writeNode by option().choice("leader", "random", "raft").default("leader")

    override fun run() {
        val loadTest = LoadTest(Workload.valueOf(workload), keyCount, clientCount) {
            KvasBackend(kvasClientFactory(), writeNode.toWriteNodeSelector())
        }
        loadTest.generateWorkload()
        kvasClientFactory().getNodeStatistics().forEach {
            println(it)
        }
        System.exit(0)
    }
}

class MapReduce : CliktCommand(name = "mapreduce") {
    val kvasClientFactory by requireObject<() -> KvasClient>()
    val script by option().file(mustExist = true, canBeDir = false).required()

    override fun run() {
        val kvasClient = kvasClientFactory()
        val mapGrpcPool = GrpcPoolImpl<MapperBlockingStub>(NodeAddress("localhost", 0)) { channel ->
            MapperGrpc.newBlockingStub(channel)

        }
        val reduceGrpcPool = GrpcPoolImpl<ReducerGrpc.ReducerBlockingStub>(NodeAddress("localhost", 0)) { channel ->
            ReducerGrpc.newBlockingStub(channel)
        }
        kvasClient.metadata.shardsList.map { it.leader }.forEach {
            reduceGrpcPool.rpc(it.nodeAddress.toNodeAddress()) {
                startReduce(startReduceRequest {
                    this.metadata = kvasClient.metadata
                    this.reduceFunction = script.readText()
                })
            }
        }
        kvasClient.metadata.shardsList.map { it.leader }.forEach {
            mapGrpcPool.rpc(it.nodeAddress.toNodeAddress()) {
                startMap(startMapRequest {
                    this.metadata = kvasClient.metadata
                    this.mapFunction = script.readText()
                })
            }
        }
    }


}

private fun String.toWriteNodeSelector() = when (this) {
    "leader" -> LEADER_NODE_SELECTOR
    "random" -> RANDOM_NODE_SELECTOR
    else -> throw IllegalArgumentException("Unknown write node selector: $this")
}

fun main(args: Array<String>) = Main().subcommands(Get(), Put(), Shell(), LoadTestCommand(), MapReduce()).main(args)
