package kvas.client

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kvas.proto.KvasMetadataProto.NodeInfo
import kvas.proto.KvasMetadataProto.ReplicatedShard

interface Backend : AutoCloseable {
    val stats: String

    fun put(key: String, value: String)
    fun get(key: String): String?
}

class DummyBackend : Backend {
    override val stats: String = ""
    override fun put(key: String, value: String) {
        runBlocking { delay(50) }
    }

    override fun get(key: String): String? {
        runBlocking { delay(50) }
        return key
    }

    override fun close() {
    }
}

class KvasBackend(private val client: KvasClient) : Backend {
    override val stats: String = ""
    override fun put(key: String, value: String) {
        client.put(key, columnName = "", value)
    }

    override fun get(key: String) = client.doGet(key)
    override fun close() {
        client.close()
    }
}
typealias BackendFactory = (Int) -> Backend
