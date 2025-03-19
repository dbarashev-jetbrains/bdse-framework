package kvas.node.raft

import io.grpc.StatusRuntimeException
import kvas.proto.KvasRaftProto.LeaderElectionRequest
import kvas.proto.KvasRaftProto.LeaderElectionResponse
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.proto.leaderElectionRequest
import kvas.proto.leaderElectionResponse
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory

typealias LeaderElectionGrpc = (NodeAddress, LeaderElectionRequest) -> LeaderElectionResponse
typealias ElectionProtocolFactory = (ClusterState, NodeState, LeaderElectionGrpc) -> ElectionProtocol
typealias ElectionProtocolProvider = Pair<String, ElectionProtocolFactory>
/**
 * All available implementations of the ElectionProtocol extension.
 */
object ElectionProtocols {
    val DEMO: ElectionProtocolProvider = "demo" to ::DemoElectionProtocol
    val REAL: ElectionProtocolProvider =
        "real" to { _: ClusterState, _: NodeState, _: LeaderElectionGrpc ->
            TODO("Task 6: implement your own RAFT election protocol")
        }

    val ALL = listOf(DEMO, REAL).toMap()
}

/**
 * Interface of the election protocol extension, with the functions to start the election round and to vote for the
 * election requests from other nodes.
 */
interface ElectionProtocol {
    /**
     * Attempts to transition the current node in the cluster to the leader role.
     *
     * This method is part of the election process within a Raft-based system. It is invoked when the node
     * determines that a leader election is required, by not hearing from the leader within the heartbeat timeout.
     * The method initiates the election process by transitioning the node to the candidate state and seeking votes
     * from other nodes in the cluster. If the node obtains enough votes to form a quorum, it becomes the
     * new leader.
     *
     * The specific implementation details of how the election is conducted, such as sending requests to
     * other nodes or handling term numbers, are defined within the election protocol's logic.
     */
    fun tryBecomeLeader()

    /**
     * Processes an election request as part of the Raft leader election process.
     *
     * The method evaluates the incoming leader election request from another node in the cluster.
     * It determines the appropriate response based on the cluster's current state, node's role,
     * and term numbers. The response generated will inform the requesting node whether this node
     * grants or denies its vote in the ongoing leader election process.
     *
     * @param request The leader election request containing information about the candidate node
     *                and its current term.
     * @return A response to the leader election request indicating whether the vote is granted
     *         or denied, along with the node's current term.
     */
    fun processElectionRequest(request: LeaderElectionRequest): LeaderElectionResponse
}


/**
 * Implementation of the ElectionProtocol for managing the leader election process in a Raft-based system.
 * This is a demo implementation that works in the basic success case.
 *
 * THIS IS NOT A CORRECT IMPLEMENTATION OF THE RAFT ELECTION PROTOCOL.
 */
class DemoElectionProtocol(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val leaderElectionRpc: LeaderElectionGrpc
) : ElectionProtocol {

    val quorumSize get() = clusterState.quorumSize

    override fun tryBecomeLeader() {
        when (nodeState.raftRole.value) {
            RaftRole.LEADER -> {
                LOG.debug("I am a LEADER.")
                return
            }
            RaftRole.CANDIDATE -> {
                LOG.debug("I am a CANDIDATE, I am not going to start elections again.")
                return
            }
            else -> {}
        }

        if (!clusterState.isLeaderDead) {
            // If we received the heartbeat within the timeout, we don't start the election.
            LOG.debug("The leader {} is not dead", clusterState.leaderAddress)
            return
        }
        LOG.debug("Trying to become a CANDIDATE...")
        nodeState.raftRole.value = RaftRole.CANDIDATE
        val electionRequest = leaderElectionRequest {
            nodeAddress = nodeState.address.toString()
            termNumber = nodeState.currentTerm
            lastLogEntryNumber = nodeState.logStorage.lastOrNull()?.entryNumber ?: LogEntryNumber.getDefaultInstance()
        }
        val grantedVotes = mutableSetOf<String>()

        val allVoters = clusterState.raftNodes
        LOG.debug("RAFT voters: {}", allVoters)

        for (address in allVoters) {
            if (grantedVotes.size >= quorumSize) {
                // Once we have enough votes, we're done.
                break

            }
            if (clusterState.isLeaderDead) {
                try {
                    val response = leaderElectionRpc(address, electionRequest)
                    if (response.isGranted) {
                        grantedVotes.add(response.voterAddress)
                    }
                } catch (ex: Throwable) {
                    if (ex is StatusRuntimeException) {
                        LOG.warn("node $address seems to be unavailable")
                    } else {
                        LOG.error("Unexpected exception while sending election request", ex)
                    }
                }
            }
        }
        if (grantedVotes.size >= quorumSize) {
            LOG.info("This node won elections with grants from {}", grantedVotes)
            nodeState.raftRole.value = RaftRole.LEADER
            clusterState.clearLeader()
        } else {
            LOG.info("This node failed to win the elections, so it is now a FOLLOWER again")
            nodeState.raftRole.value = RaftRole.FOLLOWER
        }
    }

    override fun processElectionRequest(request: LeaderElectionRequest): LeaderElectionResponse {
        LOG.info("Received election request from {}", request.nodeAddress)
        val response = if (nodeState.raftRole.value == RaftRole.FOLLOWER) {
            if (clusterState.isLeaderDead) {
                LOG.info(
                    "I am a FOLLOWER, and I suspect the leader={} is dead. My vote: YES",
                    clusterState.leaderAddress
                )
                leaderElectionResponse {
                    isGranted = true
                    voterAddress = nodeState.address.toString()
                    voterTermNumber = nodeState.currentTerm
                }
            } else {
                LOG.info("I am a FOLLOWER, and leader={} seems to be okay. My vote: NO", clusterState.leaderAddress)
                leaderElectionResponse {
                    isGranted = false
                    voterAddress = nodeState.address.toString()
                    voterTermNumber = nodeState.currentTerm
                }
            }
        } else if (request.nodeAddress == nodeState.address.toString()) {
            LOG.info("of course I will vote for myself")
            leaderElectionResponse {
                isGranted = true
                voterAddress = nodeState.address.toString()
                voterTermNumber = nodeState.currentTerm
            }
        } else {
            LOG.info("I am a {}. My vote: NO", nodeState.raftRole)
            leaderElectionResponse {
                isGranted = false
                voterAddress = nodeState.address.toString()
                voterTermNumber = nodeState.currentTerm
            }
        }
        if (response.isGranted) {
            clusterState.clearLeader()
            nodeState.votedFor = request.nodeAddress.toNodeAddress()
        }
        return response
    }
}


private val LOG = LoggerFactory.getLogger("Raft.Election")


