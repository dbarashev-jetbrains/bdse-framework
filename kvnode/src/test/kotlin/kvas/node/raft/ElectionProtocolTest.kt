package kvas.node.raft

import kvas.proto.leaderElectionRequest
import kvas.proto.leaderElectionResponse
import kvas.proto.raftAppendLogRequest
import kvas.util.toNodeAddress
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

/**
 * These tests are written for the DEMO election protocol. They are not necessary applicable to the real RAFT election
 * protocol.
 */
class ElectionProtocolTest {

    data class TestSetup(val protocol: ElectionProtocol, val clusterState: ClusterStateImpl, val nodeState: NodeStateImpl)
    private fun createDemoElectionProtocol(leaderElectionGrpc: LeaderElectionGrpc): TestSetup {
        val logStorage = InMemoryLogStorage()
        val term = AtomicInteger(1)
        val clusterState = ClusterStateImpl("127.0.0.1:9000".toNodeAddress(), listOf("127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002").map { it.toNodeAddress() })
        val nodeState = NodeStateImpl("127.0.0.1:9000".toNodeAddress(), term, logStorage)
        return TestSetup(DemoElectionProtocol(clusterState, nodeState, leaderElectionGrpc), clusterState, nodeState)
    }

    @Test
    fun `election is won with the quorum`() {
        val testSetup = createDemoElectionProtocol { address, _ ->
            leaderElectionResponse {
                this.isGranted = address.port != 9002
                this.voterAddress = address.toString()
                this.voterTermNumber = 1
            }
        }
        testSetup.protocol.tryBecomeLeader()
        assertEquals(RaftRole.LEADER, testSetup.nodeState.raftRole.value)
    }

    @Test
    fun `election is lost with no quorum`() {
        val testSetup = createDemoElectionProtocol { address, _ ->
            leaderElectionResponse {
                this.isGranted = address.port == 9000
                this.voterAddress = address.toString()
                this.voterTermNumber = 1
            }
        }
        testSetup.protocol.tryBecomeLeader()
        assertEquals(RaftRole.FOLLOWER, testSetup.nodeState.raftRole.value)
    }

    @Test
    fun `election request is granted if leader is suspected dead`() {
        val testSetup = createDemoElectionProtocol { address, _ ->
            leaderElectionResponse {
                this.isGranted = true
                this.voterAddress = address.toString()
                this.voterTermNumber = 1
            }
        }
        testSetup.clusterState.heartbeatTs.set(0)
        val resp = testSetup.protocol.processElectionRequest(leaderElectionRequest {
            this.nodeAddress = "127.0.0.1:9000"
            this.termNumber = 1
        })
        assertEquals(true, resp.isGranted)
    }

    @Test
    fun `election request is NOT granted if leader is NOT suspected dead`() {
        val testSetup = createDemoElectionProtocol { address, _ ->
            leaderElectionResponse {
                this.isGranted = true
                this.voterAddress = address.toString()
                this.voterTermNumber = 1
            }
        }
        testSetup.clusterState.updateLeader(raftAppendLogRequest {
            this.senderAddress = "127.0.0.1:9000"
            this.termNumber = 1
        })
        testSetup.clusterState.heartbeatTs.set(System.currentTimeMillis())
        val resp = testSetup.protocol.processElectionRequest(leaderElectionRequest {
            this.nodeAddress = "127.0.0.1:9000"
            this.termNumber = 1
        })
        assertEquals(false, resp.isGranted)
    }

}