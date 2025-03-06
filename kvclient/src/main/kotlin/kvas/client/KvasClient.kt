package kvas.client

import io.grpc.ManagedChannel
import kvas.proto.*
import kvas.proto.KvasMetadataProto.NodeInfo
import kvas.proto.KvasMetadataProto.ReplicatedShard
import kvas.setup.Sharding
import kvas.util.NodeAddress
import kvas.util.NodeStatistics
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException

/**
 * The KvasClient is a client for interacting with a distributed KV store.
 *
 * @param sharding The sharding strategy used to determine the corresponding shard for a given key.
 * @param metadataAddress The address of the metadata service that provides cluster information.
 * @param metadataStubFactory A factory function to produce gRPC stubs for interacting with the metadata service.
 * @param dataStubFactory A factory function to produce gRPC stubs for interacting with data services on individual nodes.
 *
 * @constructor Initializes the client, updates metadata, and prepares for subsequent operations.
 */
class KvasClient(
    private val sharding: Sharding,
    metadataAddress: NodeAddress,
    metadataStubFactory: (NodeAddress) -> MetadataServiceGrpc.MetadataServiceBlockingStub,
    private val dataStubFactory: (NodeAddress) -> DataServiceGrpc.DataServiceBlockingStub,
    private val statisticsFactory: (NodeAddress) -> StatisticsGrpc.StatisticsBlockingStub
) {
    private val metadataStub = metadataStubFactory(metadataAddress)
    private val nodeStubs = mutableMapOf<String, DataServiceGrpc.DataServiceBlockingStub>()
    private var metadata = clusterMetadata { }

    init {
        updateMetadata()
    }

    private fun updateMetadata() {
        LOG.debug("Updating metadata")
        val newMetadata = metadataStub.getNodes(getNodesRequest { }).metadata
        synchronized(this) {
            metadata = newMetadata
        }
        LOG.debug("New metadata={}", metadata)
    }

    private fun getShardForKey(key: String): ReplicatedShard {
        synchronized(this) {
            return sharding.calculateShard(key, metadata)
        }
    }

    private fun getNodeStub(address: String) = nodeStubs.getOrPut(address) {
        dataStubFactory(address.toNodeAddress())
    }

    fun get(key: String, attempt: Int = 0): String? {
        if (attempt == 5) {
            error("Failed to get a value by key $key in 5 attempts. Giving up.")
        }
        getShardForKey(key).let { shard ->
            val randomGetNode = shard.randomGetNode()
            println("GET: key=$key  node=${randomGetNode.nodeAddress}")
            val resp = getNodeStub(randomGetNode.nodeAddress).getValue(getValueRequest {
                this.rowKey = key
                this.shardToken = shard.shardToken
            })
            return when (resp.code) {
                KvasProto.GetValueResponse.StatusCode.OK -> if (resp.hasValue()) resp.value.value else null
                KvasProto.GetValueResponse.StatusCode.REFRESH_SHARDS -> {
                    LOG.info("GET: got REFRESH_SHARDS response")
                    updateMetadata()
                    get(key, attempt + 1)
                }

                KvasProto.GetValueResponse.StatusCode.STORAGE_ERROR -> {
                    LOG.error("GET: storage error with node=${randomGetNode.nodeAddress}")
                    null
                }

                else -> error("GET: unrecognized status code=${resp.code}")
            }
        }
    }

    fun put(key: String, value: String, nodeSelector: (ReplicatedShard)->NodeInfo = LEADER_NODE_SELECTOR) {
        PutTask(key, value, nodeSelector).run()
    }

    /**
     * PUT requests may be redirected, and we need to keep additional state to detect possible redirect loops.
     * That's why there is a separate object for executing a PUT request.
     */
    inner class PutTask(private val key: String, private val value: String, private val nodeSelector: (ReplicatedShard) -> NodeInfo) {
        private var refreshCount = 0
        private val redirectChain = mutableSetOf<String>()

        fun run() {
            if (refreshCount == 5) {
                error("Failed to get a value by key $key in 5 attempts. Giving up.")
            }
            getShardForKey(key).let {
                val writeNode = nodeSelector(it)
                put(writeNode, getNodeStub(writeNode.nodeAddress), key, value)
            }
        }

        private fun put(shard: NodeInfo, node: DataServiceGrpc.DataServiceBlockingStub, key: String, value: String) {
            try {
                val resp = node.putValue(putValueRequest {
                    this.rowKey = key
                    this.shardToken = shard.shardToken
                    this.value = value
                })
                when (resp.code) {
                    KvasProto.PutValueResponse.StatusCode.OK -> {}
                    KvasProto.PutValueResponse.StatusCode.REFRESH_SHARDS -> {
                        LOG.info("PUT: got REFRESH_SHARDS response from {}", shard.nodeAddress)
                        refreshCount++
                        updateMetadata()
                        run()
                    }

                    KvasProto.PutValueResponse.StatusCode.COMMIT_FAILED -> {
                        LOG.error("PUT: failed to commit $key=$value")
                    }

                    KvasProto.PutValueResponse.StatusCode.REDIRECT -> {
                        LOG.info("PUT: redirected to ${resp.leaderAddress}")
                        if (redirectChain.contains(key)) {
                            error("Redirect loop: ${resp.leaderAddress} is already in $redirectChain")
                        }
                        redirectChain.add(resp.leaderAddress)
                        put(shard, getNodeStub(resp.leaderAddress), key, value)
                    }

                    KvasProto.PutValueResponse.StatusCode.STORAGE_ERROR -> {
                        LOG.error("PUT: storage error with node=${shard.nodeAddress}")
                    }

                    else -> error("PUT: unrecognized status code=${resp.code}")
                }
            } catch (ex: ExecutionException) {
                LOG.error("PUT: failed to put key=$key value=$value")
                LOG.debug("PUT: exception", ex)
            }
        }
    }

    fun getNodeStatistics(): List<NodeStatistics> {
        val statisticsList = mutableListOf<NodeStatistics>()
        updateMetadata()
        val allNodes =
            metadata.shardsList.flatMap { shard -> listOf(shard.leader) + shard.followersList }.map { it.nodeAddress }
                .distinct()
        allNodes.forEach { address ->
            val statisticsStub = address.toNodeAddress().let(statisticsFactory::invoke)
            val statisticsResponse = statisticsStub.getStatistics(getStatisticsRequest { })
            statisticsList.add(
                NodeStatistics(
                    address.toNodeAddress(),
                    statisticsResponse.readSuccessRate,
                    statisticsResponse.writeSuccessRate,
                    statisticsResponse.readTotal,
                    statisticsResponse.writeTotal,
                )
            )
            statisticsStub.resetStatistics(resetStatisticsRequest { })
        }
        return statisticsList.toList()
    }

    fun close() {
        nodeStubs.values.mapNotNull { it.channel as? ManagedChannel }.forEach {
            it.shutdownNow()
        }
        (metadataStub.channel as? ManagedChannel)?.shutdownNow()
    }
}

val LEADER_NODE_SELECTOR: (ReplicatedShard)->NodeInfo = { it.leader }
val RANDOM_NODE_SELECTOR: (ReplicatedShard)->NodeInfo = { it.randomGetNode() }
private fun ReplicatedShard.randomGetNode() = (listOf(this.leader) + this.followersList).random()

private val LOG = LoggerFactory.getLogger("Client")