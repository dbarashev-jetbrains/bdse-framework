package kvas.node.mapreduce

import kotlinx.coroutines.runBlocking
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.InMemoryStorage
import kvas.proto.*
import kvas.proto.KvasSharedProto.DataRow
import kvas.setup.AllShardings
import kvas.util.NodeAddress
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class MapDriverTest {
    @Test
    fun `test map`() {
        val storage = InMemoryStorage()
        storage.put("line1", DEFAULT_COLUMN_NAME, "lorem ipsum dolor sit amet")
        storage.put("line2", DEFAULT_COLUMN_NAME, "consectetur adipiscing elit")
        
        val mapOutputStorage = InMemoryStorage()
        val mapper = MapperImpl(
            NodeAddress("127.0.0.1", 9000),
            storage,
            DemoMapDriver(mapOutputStorage, AllShardings.NAIVE.second),
            Runnable::run,
            { _, _ -> addMapOutputShardResponse {} }
        )
        runBlocking {
            mapper.startMap(startMapRequest {
                metadata = createMetadata(1)
                mapFunction = """
                    fun mapper(rowKey: String, values: Map<String, String>): List<Pair<String, Any?>> {
                      return listOf(rowKey to 1)
                    }
                """.trimIndent()
            })
        }
        val outputShard: Map<String, Any> = mapOutputStorage.scan().use {
            it.asSequence().associate<DataRow, String, Any> {
                row -> row.key to row.valuesMap[DEFAULT_COLUMN_NAME]!!
            }
        }
        assertEquals<Map<String, Any>>(
            mapOf("line1" to "1", "line2" to "1"),
            outputShard
        )
    }
}

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
