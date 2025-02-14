package kvas.node


import kotlinx.coroutines.runBlocking
import kvas.node.storage.InMemoryStorage
import kvas.proto.*
import kvas.proto.KvasProto.*
import kvas.setup.NaiveSharding
import kvas.util.NodeAddress
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DataNodeTest {

    private fun createMetadata(shardCount: Int) = clusterMetadata {
        (0 until shardCount).forEach { index ->
            shards.add(replicatedShard {
                leader = nodeInfo {
                    nodeAddress = "127.0.0.1:900$index"
                    isReplica = false
                    shardToken = index
                }
                shardToken = index
            })
        }
    }

    @Test
    fun `basic success case`() {
        val storage = InMemoryStorage()
        val dataNode = KvasDataNode(
            selfAddress = NodeAddress("127.0.0.1", 9000),
            storage = storage,
            sharding = NaiveSharding,
            registerNode = { _ ->
                registerNodeResponse {
                    code = RegisterNodeResponse.StatusCode.OK
                    shardToken = 0
                    metadata = createMetadata(shardCount = 1)
                }
            },
            dataTransferProtocol = VoidDataTransferProtocol()
        )
        dataNode.registerItself()
        runBlocking {
            dataNode.getValue(getValueRequest {
                this.rowKey = "1"
                this.shardToken = 0
            }).let { getResponse ->
                assertEquals(GetValueResponse.StatusCode.OK, getResponse.code)
                assertFalse(getResponse.hasValue())
            }

            dataNode.putValue(putValueRequest {
                this.rowKey = "1"
                this.shardToken = 0
                this.value = "1"
            }).let { putResponse ->
                assertEquals(PutValueResponse.StatusCode.OK, putResponse.code)
            }

            dataNode.getValue(getValueRequest {
                this.rowKey = "1"
                this.shardToken = 0
            }).let { getResponse ->
                assertEquals(GetValueResponse.StatusCode.OK, getResponse.code)
                assertTrue(getResponse.hasValue())
                assertEquals("1", getResponse.value.value)
            }
        }
    }

    @Test
    fun `sharding mismatch case 1`() {
        val storage = InMemoryStorage()
        // This is the case when the client is unaware of two shards from the very beginning
        val dataNode = KvasDataNode(
            selfAddress = NodeAddress("127.0.0.1", 9000),
            storage = storage,
            sharding = NaiveSharding,
            registerNode = { _ ->
                registerNodeResponse {
                    code = RegisterNodeResponse.StatusCode.OK
                    shardToken = 0
                    metadata = createMetadata(shardCount = 2)
                }
            },
            dataTransferProtocol = VoidDataTransferProtocol()
        )
        dataNode.registerItself()
        runBlocking {
            dataNode.putValue(putValueRequest {
                this.rowKey = "1"
                this.shardToken = NaiveSharding.calculateShard("1", createMetadata(shardCount = 1)).shardToken
                this.value = "1"
            }).let { putResponse ->
                assertEquals(PutValueResponse.StatusCode.REFRESH_SHARDS, putResponse.code)
            }
        }
    }

    @Test
    fun `sharding mismatch case 2`() {
        val storage = InMemoryStorage()
        // This is the case when the metadata on the client initially is correct, however, it changes while the client is working.
        var shardCount = 1
        val dataNode = KvasDataNode(
            selfAddress = NodeAddress("127.0.0.1", 9000),
            storage = storage,
            sharding = NaiveSharding,
            registerNode = { _ ->
                registerNodeResponse {
                    code = RegisterNodeResponse.StatusCode.OK
                    shardToken = 0
                    metadata = createMetadata(shardCount = shardCount)
                }
            },
            dataTransferProtocol = VoidDataTransferProtocol()
        )
        dataNode.registerItself()
        runBlocking {
            dataNode.putValue(putValueRequest {
                this.rowKey = "0"
                this.shardToken = NaiveSharding.calculateShard("0", createMetadata(shardCount = 1)).shardToken
                this.value = "0"
            }).let { putResponse ->
                assertEquals(PutValueResponse.StatusCode.OK, putResponse.code)
            }
        }
        // Notify the data node about the new shard
        dataNode.onShardingChange(createMetadata(shardCount = 2))

        // Let the data node send "registerItself" request.
        shardCount = 2
        dataNode.registerItself()

        // Expect REFRESH_SHARDS response.
        runBlocking {
            dataNode.putValue(putValueRequest {
                this.rowKey = "0"
                this.shardToken = NaiveSharding.calculateShard("0", createMetadata(shardCount = 1)).shardToken
                this.value = "0"
            }).let { putResponse ->
                assertEquals(PutValueResponse.StatusCode.REFRESH_SHARDS, putResponse.code)
            }
        }
    }
}