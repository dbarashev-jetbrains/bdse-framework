package kvas.node.mapreduce

import kotlinx.coroutines.runBlocking
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.InMemoryStorage
import kvas.proto.startMapRequest
import kvas.setup.AllShardings
import kvas.setup.Sharding
import kvas.util.NodeAddress
import org.junit.jupiter.api.Test

class MapDriverTest {
    @Test
    fun `test map`() {
        val storage = InMemoryStorage()
        storage.put("line1", DEFAULT_COLUMN_NAME, "lorem ipsum dolor sit amet")
        storage.put("line2", DEFAULT_COLUMN_NAME, "consectetur adipiscing elit")
        val mapper = MapperImpl(NodeAddress("127.0.0.1", 9000), storage, DemoMapDriver(storage, AllShardings.NAIVE.second), Runnable::run)
        runBlocking {
            mapper.startMap(startMapRequest {  })
        }
    }
}