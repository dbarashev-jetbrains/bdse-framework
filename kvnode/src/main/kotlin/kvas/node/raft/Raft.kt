package kvas.node.raft

import io.grpc.ManagedChannelBuilder
import kvas.node.RaftConfig
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.ElectionServiceGrpc.ElectionServiceBlockingStub
import kvas.proto.ElectionServiceGrpcKt.ElectionServiceCoroutineImplBase
import kvas.proto.KvasRaftProto.*
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.util.KvasPool
import kvas.util.NodeAddress
import kvas.util.ObservableProperty
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.timer
import kotlin.random.Random

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

interface NodeState {
    val address: NodeAddress
    var currentTerm: Int
    val lastLogEntry: LogEntryNumber
    val logStorage: InMemoryLogStorage

    var raftRole: ObservableProperty<RaftRole>
}

interface ClusterState {
    val raftNodes: List<NodeAddress>
    val quorumSize: Int get() = raftNodes.size / 2 + 1
    fun sendElectionRequest(address: NodeAddress, req: LeaderElectionRequest): LeaderElectionResponse

    val isLeaderDead: Boolean get() = false
    val leaderAddress: NodeAddress get() = NodeAddress("127.0.0.1", 9000)
    fun updateLeader(logRequest: RaftAppendLogRequest)
}

class RaftNode(
    raftConfig: RaftConfig,
    private val selfAddress: NodeAddress,
    private val delegateStorage: Storage,
    private val metadataStub: MetadataServiceGrpc.MetadataServiceBlockingStub
) {

    private val logStorage = InMemoryLogStorage()
    private val currentTerm = AtomicInteger(1)
    private val raftNodes = mutableListOf<NodeAddress>()
    private val nodeState = NodeStateImpl(selfAddress, currentTerm, logStorage)
    private val clusterState = ClusterStateImpl(selfAddress, raftNodes)
    private val electionServiceImpl = ElectionService(raftConfig, clusterState, nodeState)
    private val replicationFollower =
        RaftReplicationFollowers.ALL[raftConfig.follower]!!.invoke(clusterState, nodeState, delegateStorage)
    private val replicationLeader =
        RaftReplicationLeaders.ALL[raftConfig.leader]!!.invoke(clusterState, nodeState, delegateStorage, logStorage)

    init {
        nodeState.raftRole.subscribe { oldRole, newRole ->
            when (newRole) {
                RaftRole.LEADER -> {
                    when (oldRole) {
                        RaftRole.FOLLOWER -> error("You can't transition FOLLOWER->LEADER")
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
            println("Role: $oldRole=>$newRole")
            if (newRole == RaftRole.FOLLOWER && oldRole == RaftRole.FOLLOWER) {
                Exception().printStackTrace()
            }
            true
        }
        timer(name = "Register node", period = 10000, initialDelay = 1000) {
            metadataStub.registerNode(registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.RAFT_NODE
                nodeAddress = selfAddress.toString()
            })
        }

    }

    fun onMetadataChange(clusterMetadata: KvasMetadataProto.ClusterMetadata) {
        raftNodes.clear()
        raftNodes.addAll(clusterMetadata.raftGroup.nodesList.map { it.nodeAddress.toNodeAddress() })
        replicationLeader.onMetadataChange()
    }

    fun getElectionService(): ElectionServiceCoroutineImplBase = electionServiceImpl

    fun getFollowerService() = replicationFollower

    fun getDataService(): DataServiceGrpcKt.DataServiceCoroutineImplBase {
        return replicationLeader.getDataService()
    }
}

class NodeStateImpl(
    override val address: NodeAddress,
    private val _currentTerm: AtomicInteger,
    override val logStorage: InMemoryLogStorage
) : NodeState {
    override var currentTerm: Int
        get() = _currentTerm.get()
        set(value) = _currentTerm.set(value)

    override val lastLogEntry: LogEntryNumber
        get() = logStorage.lastOrNull()?.entryNumber ?: LogEntryNumber.getDefaultInstance()
    override var raftRole = ObservableProperty(RaftRole.FOLLOWER)
}

class ClusterStateImpl(selfAddress: NodeAddress, override val raftNodes: List<NodeAddress>) : ClusterState {
    var heartbeatTs = AtomicLong(0)
    var leaderAddress_ = NodeAddress("127.0.0.1", 9000)
    val electionPool = KvasPool<ElectionServiceBlockingStub>(selfAddress) {
        ManagedChannelBuilder.forAddress(it.host, it.port).usePlaintext().build().let { channel ->
            ElectionServiceGrpc.newBlockingStub(channel)
        }
    }

    override fun sendElectionRequest(
        address: NodeAddress,
        req: LeaderElectionRequest
    ): LeaderElectionResponse = electionPool.rpc(address) { leaderElection(req) }


    override val isLeaderDead: Boolean
        get() = System.currentTimeMillis() - heartbeatTs.get() >= HEARTBEAT_PERIOD

    override val leaderAddress: NodeAddress
        get() = leaderAddress_

    override fun updateLeader(logRequest: RaftAppendLogRequest) {
        heartbeatTs.set(System.currentTimeMillis())
        leaderAddress_ = logRequest.senderAddress.toNodeAddress()
    }
}

class ElectionService(raftConfig: RaftConfig, clusterState: ClusterState, nodeState: NodeState) :
    ElectionServiceCoroutineImplBase() {
    // Timer that initiates new leader elections.
    private val electionTimeout: Timer
    private val electionProtocol: ElectionProtocol


    init {
        electionProtocol = ElectionProtocols.ALL.getValue(raftConfig.electionProtocol).invoke(clusterState, nodeState)
        electionTimeout = timer(
            "Election Timeout", initialDelay = HEARTBEAT_PERIOD * 1.5.toLong(),
            period = Random.nextLong(HEARTBEAT_PERIOD, HEARTBEAT_PERIOD * 2).also {
                LoggerFactory.getLogger("Node.Election").info("Election timeout={}", it)
            }) {
            electionProtocol.tryBecomeLeader()
        }
    }

    override suspend fun leaderElection(request: KvasRaftProto.LeaderElectionRequest): KvasRaftProto.LeaderElectionResponse {
        return electionProtocol.processElectionRequest(request)
    }
}

internal const val HEARTBEAT_PERIOD = 2000L
