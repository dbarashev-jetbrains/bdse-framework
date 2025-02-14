package kvas.node

import com.github.ajalt.clikt.command.ChainedCliktCommand
import com.github.ajalt.clikt.command.main
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.groups.groupChoice
import com.github.ajalt.clikt.parameters.groups.mutuallyExclusiveOptions
import com.github.ajalt.clikt.parameters.groups.required
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.int
import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kvas.node.raft.*
import kvas.node.replication.ReplicationFollowerFactory
import kvas.node.replication.ReplicationLeaderFactory
import kvas.node.storage.*
import kvas.proto.KvasMetadataProto.NodeInfo
import kvas.proto.KvasProto
import kvas.proto.KvasProto.ShardingChangeRequest
import kvas.proto.MetadataListenerGrpc
import kvas.proto.MetadataServiceGrpc
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.setup.AllShardings
import kvas.setup.NaiveSharding
import kvas.setup.NotImplementedSharding
import kvas.setup.Sharding
import kvas.util.NodeAddress
import kvas.util.toNodeAddress

internal class ShardingChangeRecipient(private val address: NodeAddress) : AutoCloseable {
    val channel = ManagedChannelBuilder.forAddress(address.host, address.port).usePlaintext().build()
    val stub = MetadataListenerGrpc.newBlockingStub(channel)
    override fun close() {
        channel.shutdown()
    }
}

/**
 * Builder class for configuring and initializing a KvasNode with storage, sharding, and metadata services.
 */
class KvasNodeBuilder {
    var raftConfig: RaftConfig = RaftConfig(
        ElectionProtocols.DEMO.first,
        RaftReplicationLeaders.DEMO.first,
        AppendLogProtocols.DEMO.first,
        LogStorages.IN_MEMORY.first,
    )
    var failureEmulator: OutageEmulator? = null
    var grpcPort = 9000
        set(value) {
            field = value
            selfAddress = NodeAddress("localhost", value)
        }
    var selfAddress: NodeAddress = NodeAddress("localhost", 9000)
    var metadataConfig: MetadataConfig = MetadataConfig(isMaster = true, masterAddress = selfAddress)
    var storage: Storage = InMemoryStorage()
    var sharding: Sharding = NaiveSharding
    var dataTransferServiceImpl: String = DataTransferProtocols.DEMO.first
    var replicationConfig: ReplicationConfig = ReplicationConfig(isFollower = false, impl = "void")
    val metadataListeners = mutableListOf<OnMetadataChange>()

    private fun onShardingChange(nodes: List<NodeInfo>, request: ShardingChangeRequest) {
        nodes.forEach { node ->
            ShardingChangeRecipient(node.nodeAddress.toNodeAddress()).use { shardingChangeRecipient ->
                shardingChangeRecipient.stub.shardingChange(request)
            }
        }
    }

    private fun setupReplication(grpcBuilder: ServerBuilder<*>, metadataStub: MetadataServiceBlockingStub) {
        if (this.replicationConfig.isFollower) {
            val replicationFollower = ReplicationFollowerFactory.ALL[this.replicationConfig.impl]!!.invoke(
                this.selfAddress,
                storage,
                metadataStub
            )
            grpcBuilder.addService(replicationFollower.createGrpcService())
            this.storage = replicationFollower.storage
        } else {
            val replicationLeader = ReplicationLeaderFactory.ALL[this.replicationConfig.impl]!!.invoke(
                this.selfAddress,
                storage,
                metadataStub
            )
            this.storage = replicationLeader.createStorage()
            this.metadataListeners.add(replicationLeader.createMetadataListener())
        }
    }

    // If a failure emulator is configured, wraps the storage into a proxy that fails and recovers with the specified probabilities.
    private fun createFailingStorage(delegate: Storage): Storage =
        this.failureEmulator?.let { FailingStorage(it, delegate) } ?: delegate

    fun addServices(grpcBuilder: ServerBuilder<*>) {
        if (this.metadataConfig.isMaster) {
            grpcBuilder.addService(MetadataMaster(sharding = this.sharding, onShardingChange = this::onShardingChange))
        }
        if (raftConfig.isConfigured) {
            addRaftServices(grpcBuilder)
            return
        }

        val metadataStub = MetadataServiceGrpc.newBlockingStub(
            ManagedChannelBuilder.forAddress(metadataConfig.masterAddress.host, metadataConfig.masterAddress.port)
                .usePlaintext().build()
        )
        setupReplication(grpcBuilder, metadataStub)
        val statisticsStorage = StatisticsStorage(this.createFailingStorage(this.storage))
        val dataService = KvasDataNode(
            selfAddress = this.selfAddress, storage = statisticsStorage,
            sharding = this.sharding,
            dataTransferProtocol = DataTransferProtocols.ALL[this.dataTransferServiceImpl]!!.invoke(
                sharding,
                selfAddress,
                storage
            ),
            registerNode = {
                metadataStub.registerNode(
                    it.toBuilder()
                        .setRole(if (this.replicationConfig.isFollower) KvasProto.RegisterNodeRequest.Role.REPLICA_NODE else KvasProto.RegisterNodeRequest.Role.LEADER_NODE)
                        .build()
                )
            },
        )
        grpcBuilder.addService(dataService)
        metadataListeners.add(dataService::onShardingChange)
        grpcBuilder.addService(MetadataListenerImpl { shardingChangeRequest ->
            metadataListeners.forEach { it.invoke(shardingChangeRequest.metadata) }
        })
        grpcBuilder.addService(dataService.createDataTransferService())
        grpcBuilder.addService(StatisticsService(statisticsStorage))
    }

    fun addRaftServices(grpcBuilder: ServerBuilder<*>) {
        println("-------- RAFT is HERE! -----------")
        val metadataStub = MetadataServiceGrpc.newBlockingStub(
            ManagedChannelBuilder.forAddress(metadataConfig.masterAddress.host, metadataConfig.masterAddress.port)
                .usePlaintext().build()
        )
        val statisticsStorage = StatisticsStorage(this.createFailingStorage(this.storage))
        val raftNode = RaftNode(this.raftConfig, this.selfAddress, statisticsStorage, metadataStub)
        metadataListeners.add(raftNode::onMetadataChange)
        grpcBuilder.addService(raftNode.getElectionService())
        grpcBuilder.addService(raftNode.getFollowerService())
        val dataService = raftNode.getDataService()
        grpcBuilder.addService(dataService)
        grpcBuilder.addService(MetadataListenerImpl { shardingChangeRequest ->
            metadataListeners.forEach { it.invoke(shardingChangeRequest.metadata) }
        })
        grpcBuilder.addService(StatisticsService(statisticsStorage))
    }

    override fun toString(): String {
        return """
-------------------------------------------------------------------
Running on ${selfAddress}
* ${storage}
  Supported features: 
  ${storage.supportedFeatures}
* ${failureEmulator?.let { "Failure emulator: ${it.meanRequestsToFail} to fail, ${it.meanRequestsToRecover} to recover" } ?: "Failure emulator is OFF"}

* ${metadataConfig}
* ${sharding}
* ${replicationConfig}
* ${raftConfig}
        """.trimIndent()
    }
}

sealed class StorageConfig(name: String) : OptionGroup(name)

class PostgresConfig : StorageConfig("PostgreSQL storage options") {
    val dbHost by option().default("localhost")
    val dbUser by option().default("postgres")
    val dbPort by option().int().default(5432)
    val dbPassword by option().default("")
    val dbDatabase by option().default("")

    override fun toString(): String {
        return "$dbUser@$dbHost:$dbPort/$dbDatabase"
    }
}

class MemoryConfig : StorageConfig("Memory storage options")

class MetadataConfig(val isMaster: Boolean = true, val masterAddress: NodeAddress) {
    override fun toString(): String {
        return "MetadataConfig(isMaster=$isMaster, masterAddress=$masterAddress)"
    }
}

/**
 * This command defines if a node acts as a metadata server.
 *
 * With `--master` option it adds a metadata master service capable of keeping the cluster metadata and assigning
 * shards to the new nodes.
 * With `--address` option it configures a node as a regular cluster node that can contact and register itself in the
 * master node.
 */
class Metadata : ChainedCliktCommand<KvasNodeBuilder>() {
    val roleConfig: MetadataConfig by mutuallyExclusiveOptions(
        option("--master").flag().convert { MetadataConfig(isMaster = true, masterAddress = NodeAddress("", 0)) },
        option("--address").convert { MetadataConfig(isMaster = false, masterAddress = it.toNodeAddress()) },
    ).required()

    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    override fun run(value: KvasNodeBuilder): KvasNodeBuilder {
        value.metadataConfig = if (roleConfig.isMaster) {
            MetadataConfig(true, value.selfAddress)
        } else roleConfig
        return value
    }
}

/**
 * A command that emulates node failures and recoveries during operation.
 * The failure behavior is modeled by an `OutageEmulator` that alternates between periods
 * of successful and failed request handling.
 *
 * Options:
 * - `--mean-to-fail`: Specifies the mean number of requests served successfully before the node becomes faulty.
 * - `--mean-to-recover`: Specifies the mean number of failed requests before the node recovers and starts serving again.
 */
class Failures : ChainedCliktCommand<KvasNodeBuilder>() {
    val meanRequestsToFail by option(
        "--mean-to-fail",
        help = "The mean count of the served requests until this node becomes faulty"
    )
        .int().default(50)
    val meanRequestsToRecover by option(
        "--mean-to-recover",
        help = "The mean count of failed requests until this node recovers"
    )
        .int().default(5)

    override fun run(value: KvasNodeBuilder): KvasNodeBuilder {
        value.failureEmulator = OutageEmulator(meanRequestsToFail, meanRequestsToRecover)
        return value
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Commands and flags related to replication
class ReplicationConfig(val isFollower: Boolean, val impl: String) {
    override fun toString(): String {
        return """Replication: ${if (isFollower) "follower" else "leader"} using $impl implementation"""
    }
}

class Replication : ChainedCliktCommand<KvasNodeBuilder>() {
    val role by option("--role").choice(
        "leader", "follower"
    ).default("leader")
    val impl by option("--impl").choice("void", "naive", "async", "leaderless").default("naive")

    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    override fun run(value: KvasNodeBuilder): KvasNodeBuilder {
        value.replicationConfig = ReplicationConfig(role == "follower", impl)
        return value
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Commands and flags related to RAFT
class RaftConfig(val electionProtocol: String, val leader: String, val follower: String, val logImpl: String) {
    var isConfigured: Boolean = false
    override fun toString(): String {
        return """Raft: $electionProtocol election, $leader => $follower log replication"""
    }
}

class Raft : ChainedCliktCommand<KvasNodeBuilder>() {
    val electionProtocol by option("--election-protocol").choice(*ElectionProtocols.ALL.keys.toTypedArray())
        .default(ElectionProtocols.DEMO.first)
    val follower by option("--follower").choice(*AppendLogProtocols.ALL.keys.toTypedArray())
        .default(AppendLogProtocols.DEMO.first)
    val leader by option("--leader").choice(*RaftReplicationLeaders.ALL.keys.toTypedArray())
        .default(RaftReplicationLeaders.DEMO.first)
    val logImpl by option("--log").choice(*LogStorages.ALL.keys.toTypedArray()).default(LogStorages.IN_MEMORY.first)

    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    override fun run(value: KvasNodeBuilder): KvasNodeBuilder {
        value.raftConfig = RaftConfig(electionProtocol, leader, follower, logImpl)
        value.raftConfig.isConfigured = true
        return value
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Other options
class Main : ChainedCliktCommand<KvasNodeBuilder>() {
    override val allowMultipleSubcommands: Boolean = true
    val grpcPort by option().int().default(9000)
    val selfAddress by option()
    val storageConfig by option("--storage").groupChoice(
        "dbms" to PostgresConfig(),
        "memory" to MemoryConfig(),
    )
    val shardingConfig by option("--sharding").choice(
        *AllShardings.ALL.keys.toTypedArray()
    ).default(AllShardings.NAIVE.first)
    val dataTransfer by option("--data-transfer").choice(
        *DataTransferProtocols.ALL.keys.toTypedArray()
    ).default(DataTransferProtocols.DEMO.first)

    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true) }
        }
    }

    override fun run(kvasNodeBuilder: KvasNodeBuilder): KvasNodeBuilder {
        kvasNodeBuilder.grpcPort = grpcPort
        kvasNodeBuilder.selfAddress = this.selfAddress?.toNodeAddress() ?: "localhost:$grpcPort".toNodeAddress()

        when (val it = storageConfig) {
            is PostgresConfig -> {
                globalPostgresConfig = it
                kvasNodeBuilder.storage = DatabaseStorage(createDataSource(it), it)
            }

            is MemoryConfig -> {}
            null -> error("No storage configured")
        }

        kvasNodeBuilder.sharding = AllShardings.ALL[shardingConfig] ?: NotImplementedSharding
        kvasNodeBuilder.dataTransferServiceImpl = dataTransfer
        return kvasNodeBuilder
    }
}

/**
 * The main function initializes and starts the KvasNode server using the given arguments.
 *
 * @param args The command-line arguments for node configuration.
 */
fun main(args: Array<String>) {
    val command = Main()
    command.subcommands(Metadata(), Replication(), Failures(), Raft())
    val kvasBuilder = command.main(args, KvasNodeBuilder())
    println(kvasBuilder)
    ServerBuilder.forPort(kvasBuilder.grpcPort).let {
        kvasBuilder.addServices(it)
        it.build().start().awaitTermination()
    }
}