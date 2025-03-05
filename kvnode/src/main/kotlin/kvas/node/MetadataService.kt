package kvas.node

import com.google.protobuf.Int32Value
import kotlinx.coroutines.runBlocking
import kvas.proto.*
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.proto.KvasMetadataProto.NodeInfo
import kvas.proto.KvasProto.*
import kvas.proto.KvasProto.RegisterNodeRequest.Role
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.setup.Sharding
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory
import kotlin.concurrent.timer

typealias OnMetadataChange = (ClusterMetadata) -> Unit

/**
 * A group of nodes that serve data from the same logical shard. There is a single leader node and 0+ follower replica nodes.
 */
data class ReplicaGroup(val shardToken: Int, val leader: NodeAddress, val followers: MutableList<NodeAddress>) {
    fun asNodeInfoList(): List<NodeInfo> {
        return listOf(nodeInfo {
            nodeAddress = this@ReplicaGroup.leader.toString()
            shardToken = this@ReplicaGroup.shardToken
            isReplica = false
        }) + this.followers.map {
            nodeInfo {
                nodeAddress = it.toString()
                shardToken = this@ReplicaGroup.shardToken
                isReplica = true
            }
        }.toList()
    }

    fun asProto(withTimestamps: Boolean) = replicatedShard {
        this.shardToken = this@ReplicaGroup.shardToken
        this.leader = nodeInfo {
            this.nodeAddress = this@ReplicaGroup.leader.toString()
            this.shardToken = this@ReplicaGroup.shardToken
            this.isReplica = false
            if (withTimestamps) {
                this.lastHeartbeatTs = this@ReplicaGroup.leader.lastHeartbeat
            }
        }
        this.followers.addAll(this@ReplicaGroup.followers.map {
            nodeInfo {
                this.nodeAddress = it.toString()
                this.shardToken = this@ReplicaGroup.shardToken
                this.isReplica = true
                if (withTimestamps) {
                    this.lastHeartbeatTs = it.lastHeartbeat
                }
            }
        })
    }
}

typealias OnShardingChange = (List<NodeInfo>, ShardingChangeRequest) -> Unit
typealias KvasClock = () -> Long

/**
 * Implements a simple metadata server that keeps all its data in memory.
 */
class MetadataMaster(
    private val sharding: Sharding,
    private val clock: KvasClock = { System.currentTimeMillis() },
    private val onShardingChange: OnShardingChange
) : MetadataServiceGrpcKt.MetadataServiceCoroutineImplBase() {

    private val token2replicaGroup = mutableMapOf<Int, ReplicaGroup>()
    private val raftNodes = mutableListOf<String>()
    private val allNodes
        get() = token2replicaGroup.values.flatMap { it.asNodeInfoList() }
            .toList() + raftNodes.map { nodeInfo { nodeAddress = it } }

    private fun buildMetadataProto(withTimestamps: Boolean = false) = clusterMetadata {
        this.shards.addAll(token2replicaGroup.entries.sortedBy { it.key }.map { it.value.asProto(withTimestamps) })
        this.raftGroup = raftGroup {
            this.nodes.addAll(raftNodes.sorted().map {
                nodeInfo {
                    nodeAddress = it
                }
            })
        }
    }

    override suspend fun getNodes(request: GetNodesRequest): GetNodesResponse = getNodesResponse {
        this.metadata = buildMetadataProto()
    }

    override suspend fun registerNode(request: RegisterNodeRequest): RegisterNodeResponse = try {
        synchronized(token2replicaGroup) {
            val metadataBefore = buildMetadataProto()
            val result = when (request.role) {
                RegisterNodeRequest.Role.LEADER_NODE -> registerShardLeader(request)
                RegisterNodeRequest.Role.REPLICA_NODE -> registerShardReplica(request)
                RegisterNodeRequest.Role.RAFT_NODE -> registerRaftNode(request)
                else -> {
                    LOGGER.error("Unknown role: ${request.role}")
                    error("Unknown role: ${request.role}")
                }
            }
            val metadataAfter = buildMetadataProto()
            if (metadataBefore != metadataAfter) {
                onShardingChange()
            }
            return result.toBuilder().setMetadata(buildMetadataProto(withTimestamps = true)).build()
        }
    } catch (ex: Throwable) {
        LOGGER.error("Failure in registerNode(): ", ex)
        throw ex
    }

    private fun registerRaftNode(request: RegisterNodeRequest): RegisterNodeResponse {
        if (!raftNodes.contains(request.nodeAddress)) {
            raftNodes.add(request.nodeAddress)
        }
        LOGGER.debug("Registered a raft node. All nodes: {}", raftNodes.joinToString(","))
        return registerNodeResponse {
            code = RegisterNodeResponse.StatusCode.OK
            metadata = buildMetadataProto()
        }
    }

    private fun registerShardLeader(request: RegisterNodeRequest): RegisterNodeResponse {
        if (request.hasShardToken()) {
            val nodeAddress = request.nodeAddress.toNodeAddress()
            var replicaGroup = token2replicaGroup[request.shardToken.value]
            return if (replicaGroup == null) {
                replicaGroup = ReplicaGroup(
                    request.shardToken.value,
                    nodeAddress.also { it.lastHeartbeat = clock() },
                    mutableListOf()
                )
                token2replicaGroup[request.shardToken.value] = replicaGroup
                LOGGER.info("Registered $nodeAddress as a leader of a new replica group with token=${request.shardToken}")
                registerNodeResponse {
                    code = RegisterNodeResponse.StatusCode.OK
                    shardToken = request.shardToken.value
                    metadata = buildMetadataProto()
                }
            } else {
                if (nodeAddress != replicaGroup.leader) {
                    LOGGER.warn("Conflict: $nodeAddress and ${replicaGroup.leader} both claim to be leaders of a replica group with token=${request.shardToken}")
                    registerNodeResponse {
                        code = RegisterNodeResponse.StatusCode.TOKEN_CONFLICT
                    }
                } else {
                    LOGGER.debug(
                        "Confirmed {} as a leader of a replica group with token={}",
                        nodeAddress,
                        request.shardToken
                    )
                    replicaGroup.leader.lastHeartbeat = clock()
                    registerNodeResponse {
                        code = RegisterNodeResponse.StatusCode.OK
                        shardToken = request.shardToken.value
                        metadata = buildMetadataProto()
                    }
                }
            }
        } else {
            val newToken = sharding.createNewToken(token2replicaGroup.values.map { replicaGroup ->
                nodeInfo {
                    this.shardToken = replicaGroup.shardToken
                    this.isReplica = false
                    this.nodeAddress = replicaGroup.leader.toString()
                }
            })
            LOGGER.info("Created a new token=$newToken for ${request.nodeAddress}")
            return registerShardLeader(request.toBuilder().setShardToken(Int32Value.of(newToken)).build())
        }
    }

    private fun onShardingChange() {
        val shardingChangeRequest = shardingChangeRequest {
            this.metadata = this@MetadataMaster.buildMetadataProto(withTimestamps = true)
        }
        onShardingChange(allNodes, shardingChangeRequest)
    }

    private fun registerShardReplica(request: RegisterNodeRequest): RegisterNodeResponse {
        if (request.hasShardToken()) {
            val replicaAddress = request.nodeAddress.toNodeAddress()
            return token2replicaGroup[request.shardToken.value]?.let { replicaGroup ->
                if (!replicaGroup.followers.contains(replicaAddress)) {
                    replicaGroup.followers.add(replicaAddress)
                }
                replicaGroup.followers.find { it == replicaAddress }?.lastHeartbeat = clock()
                registerNodeResponse {
                    code = RegisterNodeResponse.StatusCode.OK
                    shardToken = request.shardToken.value
                    metadata = buildMetadataProto()
                }
            } ?: registerNodeResponse {
                code = RegisterNodeResponse.StatusCode.TOKEN_UNKNOWN
            }
        } else {
            val minFollowers = token2replicaGroup.minBy { it.value.followers.size }
            minFollowers.value.followers.add(request.nodeAddress.toNodeAddress().also {
                it.lastHeartbeat = clock()
            })
            return registerNodeResponse {
                code = RegisterNodeResponse.StatusCode.OK
                shardToken = minFollowers.key
                metadata = buildMetadataProto()
            }
        }
    }
}

class RegisterTask(private val metadataStub: MetadataServiceBlockingStub,
                   private val selfAddress: NodeAddress,
                   private val role: Role,
                   private val getShardToken: () -> Int?,
                   private val onRegister: (RegisterNodeResponse) -> Unit) {
    init {
        // We will send registration requests to the metadata server once per 10 seconds.
        timer(name = "Register node", period = 10000, initialDelay = 1000) {
            registerItself()
        }
    }

    private fun registerNode(): RegisterNodeResponse =
        metadataStub.registerNode(registerNodeRequest {
            this.nodeAddress = selfAddress.toString()
            role = this@RegisterTask.role
            getShardToken()?.let { this.shardToken = Int32Value.of(it) }
        })


    internal fun registerItself() {
        val log = LoggerFactory.getLogger("Node.RegisterItself")
        // Register at the metadata server, supplying the assigned token if it is available.
        val response = registerNode()
        when (response.code) {
            KvasProto.RegisterNodeResponse.StatusCode.OK -> {
                log.debug("Registered with token={}", response.shardToken)
                onRegister(response)
            }
            else -> {
                log.error("Can't register at the metadata master, received {}", response)
                System.exit(1)
            }
        }
    }
}
private val LOGGER = LoggerFactory.getLogger("MetadataService.Master")