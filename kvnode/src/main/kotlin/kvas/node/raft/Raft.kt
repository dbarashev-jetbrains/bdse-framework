package kvas.node.raft

import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kvas.node.RaftConfig
import kvas.node.storage.ClusterOutageState
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.ElectionServiceGrpc.ElectionServiceBlockingStub
import kvas.proto.ElectionServiceGrpcKt.ElectionServiceCoroutineImplBase
import kvas.proto.KvasRaftProto.*
import kvas.util.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.timer
import kotlin.random.Random

/**
 * Represents the roles that a node can take in a Raft-based distributed consensus algorithm.
 */
enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

/**
 * Represents the state of a node in a Raft-based distributed consensus system.
 */
interface NodeState {
    /**
     * The network address of the node, represented by its host and port values.
     */
    val address: NodeAddress

    /**
     * The current term in the Raft protocol cycle that the node is participating in. This property is mutable and can be
     * changed from the node implementation.
     */
    var currentTerm: Int

    /**
     * A candidate this node voted for in the current term.
     */
    var votedFor: NodeAddress?

    /**
     * Represents the log storage for maintaining the replicated log entries on the node.
     */
    val logStorage: LogStorage

    /**
     * The current role of the node (follower, candidate, or leader). This can be changed based on the
     * state of the Raft algorithm and can be observed to take the appropriate actions when the state changes.
     */
    val raftRole: ObservableProperty<RaftRole>
}

/**
 * Represents the state of a cluster in a Raft-based system, providing details about the nodes, quorum size,
 * leader status, and methods to enable communication and updates within the cluster.
 */
interface ClusterState {
    /**
     * The list of all nodes in a Raft cluster. The contents of this list may change as new nodes are added.
     */
    val raftNodes: List<NodeAddress>

    /**
     * The quorum size in this cluster. The value may change as new nodes are added.
     */
    val quorumSize: Int get() = raftNodes.size / 2 + 1

    /**
     * Returns true if the leader node is suspected to be dead (we have not heard from the leader for a while)
     */
    val isLeaderDead: Boolean get() = false

    /**
     * The current leader address.
     */
    val leaderAddress: NodeAddress get() = NodeAddress("localhost", 0)

    /**
     * Updates leader information using the received log replication request.
     */
    fun updateLeader(logRequest: RaftAppendLogRequest)
    fun clearLeader() {
        updateLeader(raftAppendLogRequest {
            this.senderAddress = ":0"
        })
    }
}

/**
 * This is a Raft node instance. It initializes the state objects and implementations of different parts of the Raft
 * protocol, registers this node in the metadata and sets up participation in the leader elections.
 */
class RaftNode(
    raftConfig: RaftConfig,
    private val selfAddress: NodeAddress,
    private val delegateStorage: Storage,
    private val metadataStub: MetadataServiceGrpc.MetadataServiceBlockingStub,
    private val clusterOutageState: ClusterOutageState
) {

    private val log = LoggerFactory.getLogger("Raft.Role")
    private val logStorage = LogStorages.ALL[raftConfig.logImpl]!!.invoke()
    private val currentTerm = AtomicInteger(1)

    private val raftNodes = mutableListOf<NodeAddress>()
    private val nodeState = NodeStateImpl(selfAddress, currentTerm, logStorage)
    private val clusterState = ClusterStateImpl(selfAddress, raftNodes)
    private val electionServiceImpl = ElectionService(raftConfig, clusterState, nodeState, clusterOutageState)
    private val replicationFollower =
        RaftFollowers.ALL[raftConfig.follower]!!.invoke(clusterState, nodeState, delegateStorage)
    private val replicationLeader =
        RaftLeaders.ALL[raftConfig.leader]!!.invoke(clusterState, nodeState, delegateStorage, logStorage, clusterOutageState)

    init {
        nodeState.raftRole.subscribe { oldRole, newRole ->
            when (newRole) {
                RaftRole.LEADER -> {
                    when (oldRole) {
                        RaftRole.FOLLOWER -> error("You can't transition FOLLOWER->LEADER")
                        RaftRole.CANDIDATE -> log.info("!!! Node {} transitioned to LEADER role.", selfAddress)
                        else -> {}
                    }
                }

                RaftRole.FOLLOWER -> {}
                RaftRole.CANDIDATE -> {
                    when (oldRole) {
                        RaftRole.LEADER -> error("You can't transition LEADER->CANDIDATE")
                        else -> {}
                    }
                }
            }
            log.info("$oldRole=>$newRole")
            if (newRole == RaftRole.FOLLOWER && oldRole == RaftRole.FOLLOWER) {
                Exception().printStackTrace()
            }
            true
        }
        timer(name = "Register node", period = 10000, initialDelay = 1000) {
            try {
                metadataStub.registerNode(registerNodeRequest {
                    role = KvasProto.RegisterNodeRequest.Role.RAFT_NODE
                    nodeAddress = selfAddress.toString()
                })
            } catch (ex: Throwable) {
                log.error("Failed to register at the metadata service", ex)
            }
        }
    }

    fun onMetadataChange(clusterMetadata: KvasMetadataProto.ClusterMetadata) {
        raftNodes.clear()
        raftNodes.addAll(clusterMetadata.raftGroup.nodesList.map { it.nodeAddress.toNodeAddress() })
        replicationLeader.onMetadataChange()
    }

    fun getElectionService(): ElectionServiceCoroutineImplBase = electionServiceImpl

    internal fun getFollowerService() = RaftReplicationFollowerImpl(replicationFollower, nodeState, clusterOutageState)

    fun getDataService(): DataServiceGrpcKt.DataServiceCoroutineImplBase {
        return replicationLeader.getDataService()
    }
}

/**
 * Implementation of the follower gRPC service.
 */
internal class RaftReplicationFollowerImpl(
    private val appendLogProtocol: AppendLogProtocol,
    private val nodeState: NodeState,
    private val clusterOutageState: ClusterOutageState) : RaftReplicationServiceGrpcKt.RaftReplicationServiceCoroutineImplBase() {

    override suspend fun appendLog(request: RaftAppendLogRequest): RaftAppendLogResponse {
        if (clusterOutageState.isAvailable) {
            return appendLogProtocol.appendLog(request)
        } else throw RuntimeException("Node is not online")
    }
}

/**
 * Implementation of the node state interface.
 */
class NodeStateImpl(
    override val address: NodeAddress,
    private val _currentTerm: AtomicInteger,
    override val logStorage: LogStorage,
) : NodeState {
    override var currentTerm: Int
        get() = _currentTerm.get()
        set(value) {
            if (value < _currentTerm.get()) {
                error("New term $value is lower than current term ${_currentTerm.get()}")
            } else _currentTerm.set(value)
        }

    override var votedFor: NodeAddress? = null

    override val raftRole = ObservableProperty(RaftRole.FOLLOWER)
}

/**
 * Implementation of the cluster state interface. It keeps track of the leader address and aliveness and holds the list of
 * this Raft cluster nodes.
 */
class ClusterStateImpl(selfAddress: NodeAddress, override val raftNodes: List<NodeAddress>) : ClusterState {
    var heartbeatTs = AtomicLong(0)
    var leaderAddress_ = NodeAddress("127.0.0.1", 0)

    override val isLeaderDead: Boolean
        get() = leaderAddress_.port == 0 || System.currentTimeMillis() - heartbeatTs.get() >= HEARTBEAT_PERIOD

    override val leaderAddress: NodeAddress
        get() = leaderAddress_

    override fun updateLeader(logRequest: RaftAppendLogRequest) {
        heartbeatTs.set(System.currentTimeMillis())
        leaderAddress_ = logRequest.senderAddress.toNodeAddress()
    }
}

/**
 * Implementation of a gRPC service that handles leader elections in Raft protocol. It initiates leader election when
 * the election timeout expires and accepts the incoming election requests. It delegates the real work to the
 * ElectionProtocol implementation.
 */
class ElectionService(
    raftConfig: RaftConfig,
    clusterState: ClusterState,
    nodeState: NodeState,
    private val clusterOutageState: ClusterOutageState
) : ElectionServiceCoroutineImplBase() {

    private val electionPool = GrpcPoolImpl<ElectionServiceBlockingStub>(nodeState.address) {
        channel -> ElectionServiceGrpc.newBlockingStub(channel)
    }

    // Timer that initiates new leader elections.
    private val electionTimeout: Timer
    // The election protocol implementation.
    private val electionProtocol: ElectionProtocol

    /**
     * Sends a gRPC LeaderElection request unless the destination node is configured as unavailable from this node.
     */
    private fun sendElectionRequest(address: NodeAddress, req: LeaderElectionRequest): LeaderElectionResponse {
        if (clusterOutageState.unavailableNodes.contains(address)) {
           throw StatusRuntimeException(Status.UNAVAILABLE)
        }
        return electionPool.rpc(address) { leaderElection(req) }
    }

    init {
        electionProtocol = ElectionProtocols.ALL.getValue(raftConfig.electionProtocol)
            .invoke(clusterState, nodeState, ::sendElectionRequest)
        electionTimeout = timer(
            "Election Timeout",
            initialDelay = (HEARTBEAT_PERIOD * 1.5).toLong(),
            period = Random.nextLong(HEARTBEAT_PERIOD, HEARTBEAT_PERIOD * 2).also {
                LOG_ELECTION.info("Election timeout={}", it)
            }) {
            if (clusterOutageState.isAvailable) {
                electionProtocol.tryBecomeLeader()
            }
        }
    }

    /**
     * We will serve the leader election request unless this node is configured as unavailable from any node.
     */
    override suspend fun leaderElection(request: LeaderElectionRequest): LeaderElectionResponse {
        if (clusterOutageState.isAvailable) {
            return try {
                electionProtocol.processElectionRequest(request)
            } catch (ex: Throwable) {
                LOG_ELECTION.error("Failed to process election request", ex)
                throw ex
            }
        } else {
            throw StatusRuntimeException(Status.UNAVAILABLE)
        }
    }
}

internal val LOG_ELECTION = LoggerFactory.getLogger("Raft.Election")
internal const val HEARTBEAT_PERIOD = 5000L
