package kvas.node

import com.google.protobuf.Int32Value
import kotlinx.coroutines.runBlocking
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.proto.KvasProto
import kvas.proto.KvasProto.RegisterNodeResponse.StatusCode
import kvas.proto.getNodesRequest
import kvas.proto.nodeInfo
import kvas.proto.registerNodeRequest
import kvas.setup.NaiveSharding
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test


class MetadataMasterTest {
    private val VOID_SHARDING_CHANGE: OnShardingChange = { _, _ -> }
    private fun createMetadataMaster() =
        MetadataMaster(sharding = NaiveSharding, onShardingChange = VOID_SHARDING_CHANGE)

    @Test
    fun `register shard leader`() {
        runBlocking {
            val server = createMetadataMaster()
            val request = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9000"
            }

            // First request is supposed to add a leader record.
            server.registerNode(request).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000"), response.metadata.shardsList.map { it.leader.nodeAddress })
            }

            // Second request is supposed to succeed, as the address and the shard token are the same.
            server.registerNode(request).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000"), response.metadata.shardsList.map { it.leader.nodeAddress })
            }
        }
    }

    @Test
    fun `register shard replica`() {
        runBlocking {
            val server = createMetadataMaster()
            val leaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9000"
            }
            val followerRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9001"
            }

            // Registering a follower will fail if there is no leader yet.
            server.registerNode(followerRequest).let { response ->
                assertEquals(StatusCode.TOKEN_UNKNOWN, response.code)
            }

            // Registering a follower when the leader is there will succeed.
            server.registerNode(leaderRequest)
            server.registerNode(followerRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000", "127.0.0.1:9001"), response.metadata.toNodeAddressList())
            }

            // After that any leader/follower registration requests are idempotent.
            server.registerNode(followerRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000", "127.0.0.1:9001"), response.metadata.toNodeAddressList())
            }

            server.registerNode(leaderRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000", "127.0.0.1:9001"), response.metadata.toNodeAddressList())
            }

            // The second follower adds to the list of followers.
            val secondFollowerRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9002"
            }
            server.registerNode(secondFollowerRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(
                    listOf("127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"),
                    response.metadata.toNodeAddressList()
                )
            }

            // A node that is trying to register as a follower in another shard will fail.
            val anotherShardFollowerRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
                shardToken = Int32Value.of(1000)
                nodeAddress = "127.0.0.1:9003"
            }
            server.registerNode(anotherShardFollowerRequest).let { response ->
                assertEquals(StatusCode.TOKEN_UNKNOWN, response.code)
            }
        }
    }

    @Test
    fun `register conflicting leaders`() {
        runBlocking {
            val server = createMetadataMaster()
            val leaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9000"
            }
            server.registerNode(leaderRequest)

            val conflictingLeaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:8000"
            }
            server.registerNode(conflictingLeaderRequest).let { response ->
                assertEquals(StatusCode.TOKEN_CONFLICT, response.code)
            }
        }
    }

    @Test
    fun `register two shard leaders`() {
        runBlocking {
            val server = createMetadataMaster()

            val shard1leaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(0)
                nodeAddress = "127.0.0.1:9000"
            }
            server.registerNode(shard1leaderRequest)

            val shard2LeaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                shardToken = Int32Value.of(1000)
                nodeAddress = "127.0.0.1:8000"
            }
            server.registerNode(shard2LeaderRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000", "127.0.0.1:8000"), response.metadata.toNodeAddressList())
            }
        }
    }

    @Test
    fun `register many leaders and followers`() {
        val server = createMetadataMaster()
        runBlocking {
            (0..3).forEach { idx ->
                val resp = server.registerNode(registerNodeRequest {
                    role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                    nodeAddress = "127.0.0.1:${9000 + idx * 1000}"
                    shardToken = Int32Value.of(idx * 1000)
                })
                assertEquals(StatusCode.OK, resp.code)
            }
            (0..3).forEach { idx ->
                val resp = server.registerNode(registerNodeRequest {
                    role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
                    nodeAddress = "127.0.0.1:${9001 + idx * 1000}"
                    shardToken = Int32Value.of(idx * 1000)
                })
                assertEquals(StatusCode.OK, resp.code)
            }
            assertEquals(
                listOf(9000, 9001, 10000, 10001, 11000, 11001, 12000, 12001).map { "127.0.0.1:$it" }.toList(),
                server.getNodes(getNodesRequest { }).metadata.toNodeAddressList()
            )
        }
    }

    @Test
    fun `register shard leader with unassigned token`() {
        runBlocking {
            val server = createMetadataMaster()
            val leaderRequest = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                nodeAddress = "127.0.0.1:9000"
            }
            server.registerNode(leaderRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(0, response.shardToken)
                assertEquals(listOf("127.0.0.1:9000"), response.metadata.toNodeAddressList())
            }
        }
    }

    @Test
    fun `receive metadata change event`() {
        runBlocking {
            var expectedRecipients = listOf(nodeInfo { nodeAddress = "127.0.0.1:9000" })
            var eventReceived = false
            val server = MetadataMaster(NaiveSharding, onShardingChange = { nodes, shardingChange ->
                assertEquals(expectedRecipients, nodes)
                eventReceived = true
            })
            val leader1Request = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                nodeAddress = "127.0.0.1:9000"
            }
            server.registerNode(leader1Request)
            assertTrue(eventReceived)


            expectedRecipients = listOf(
                nodeInfo { nodeAddress = "127.0.0.1:9000" },
                nodeInfo {
                    nodeAddress = "127.0.0.1:9001"
                    shardToken = 1
                }
            )
            val leader2Request = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
                nodeAddress = "127.0.0.1:9001"
            }
            eventReceived = false
            server.registerNode(leader2Request)
            assertTrue(eventReceived)

            val follower1Request = registerNodeRequest {
                role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
                nodeAddress = "127.0.0.1:9002"
                shardToken = Int32Value.of(1)
            }
            expectedRecipients = listOf(
                nodeInfo { nodeAddress = "127.0.0.1:9000" },
                nodeInfo {
                    nodeAddress = "127.0.0.1:9001"
                    shardToken = 1
                },
                nodeInfo {
                    nodeAddress = "127.0.0.1:9002"
                    shardToken = 1
                    this.isReplica = true
                }
            )
            eventReceived = false
            server.registerNode(follower1Request)
            assertTrue(eventReceived)
        }
    }

    @Test
    fun `basic heartbeat test`() {
        var logicalTs = 10L
        val master =
            MetadataMaster(sharding = NaiveSharding, onShardingChange = VOID_SHARDING_CHANGE, clock = { logicalTs })
        val request = registerNodeRequest {
            role = KvasProto.RegisterNodeRequest.Role.LEADER_NODE
            shardToken = Int32Value.of(0)
            nodeAddress = "127.0.0.1:9000"
        }
        val followerRequest = registerNodeRequest {
            role = KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
            shardToken = Int32Value.of(0)
            nodeAddress = "127.0.0.1:9001"
        }

        runBlocking {
            master.registerNode(request).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000"), response.metadata.shardsList.map { it.leader.nodeAddress })
                assertEquals(10L, response.metadata.shardsList[0].leader.lastHeartbeatTs)
            }
            logicalTs = 20

            master.registerNode(request).let { response ->
                assertEquals(StatusCode.OK, response.code)
                assertEquals(listOf("127.0.0.1:9000"), response.metadata.shardsList.map { it.leader.nodeAddress })
                assertEquals(20L, response.metadata.shardsList[0].leader.lastHeartbeatTs)
            }

            logicalTs = 30
            master.registerNode(followerRequest).let { response ->
                assertEquals(StatusCode.OK, response.code)
                // 20 is the last timestamp when the leader registered itself
                assertEquals(20L, response.metadata.shardsList[0].leader.lastHeartbeatTs)
                // 30 is the moment when the follower registered
                assertEquals(30L, response.metadata.shardsList[0].followersList[0].lastHeartbeatTs)
            }

        }
    }
}

private fun ClusterMetadata.toNodeAddressList() =
    this.shardsList.flatMap { listOf(it.leader.nodeAddress) + it.followersList.map { it.nodeAddress } }