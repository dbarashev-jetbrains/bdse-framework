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

        val sharedStorage = InMemoryStorage()
        val mapOutputStorage = InMemoryStorage()
        val mapper = MapperImpl(
            NodeAddress("127.0.0.1", 9000),
            storage,
            DemoMapDriver(mapOutputStorage, AllShardings.NAIVE.second),
            Runnable::run,
            { _, _ -> addMapOutputShardResponse {} },
            {sharedStorage},
        )
        runBlocking {
            mapper.startMap(startMapRequest {
                metadata = createMetadata(1)
                mapFunction = """
                    fun mapper(rowKey: String, values: Map<String, String>, storage: kvas.node.storage.Storage): List<Pair<String, Any?>> {
                      return listOf(rowKey to 1)
                    }
                """.trimIndent()
            })
        }
        val outputShard: Map<String, Any> = mapOutputStorage.scan().use {
            it.asSequence().associate<DataRow, String, Any?> {
                row -> row.key to row.valuesMap["value0"]
            }
        }.mapNotNull { (k, v) -> v?.let { k to v } }.toMap()
        assertEquals<Map<String, Any>>(
            mapOf("line1" to "1", "line2" to "1"),
            outputShard
        )
    }

    @Test
    fun `test duplicated values for key`() {
        val storage = InMemoryStorage()
        storage.put("line1", DEFAULT_COLUMN_NAME, "lorem ipsum")
        storage.put("line2", DEFAULT_COLUMN_NAME, "lorem ipsum")

        val sharedStorage = InMemoryStorage()
        val mapOutputStorage = InMemoryStorage()
        val mapper = MapperImpl(
            NodeAddress("127.0.0.1", 9000),
            storage,
            DemoMapDriver(mapOutputStorage, AllShardings.NAIVE.second),
            Runnable::run,
            { _, _ -> addMapOutputShardResponse {} },
            {sharedStorage},
        )
        runBlocking {
            mapper.startMap(startMapRequest {
                metadata = createMetadata(1)
                mapFunction = """
                    fun mapper(rowKey: String, values: Map<String, String>, storage: kvas.node.storage.Storage): List<Pair<String, Any?>> {
                      return values[""]!!.split(" ").map { it to 1 }.toList()
                    }
                """.trimIndent()
            })
        }
        val outputShard = mapOutputStorage.scan().use {
            it.asSequence().toSet()
        }
        assertEquals(setOf(
            DataRow.newBuilder().setKey("lorem")
                .putValues("value0", "1")
                .putValues("value1", "1").putValues("valueCount", "2").setVersion(4).build(),
            DataRow.newBuilder().setKey("ipsum")
                .putValues("value0", "1")
                .putValues("value1", "1").putValues("valueCount", "2").setVersion(4).build()),
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
