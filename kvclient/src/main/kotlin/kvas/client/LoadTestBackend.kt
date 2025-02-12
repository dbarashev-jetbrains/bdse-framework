package kvas.client

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

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
        client.put(key, value)
    }

    override fun get(key: String) = client.get(key)
    override fun close() {
        client.close()
    }
}
typealias BackendFactory = (Int) -> Backend
