package kvas.client

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import kotlin.math.roundToInt
import kotlin.time.Duration
import kotlin.time.measureTime

enum class Workload {
    MIXED, READONLY, SEQUENTIAL
}

data class Payload(val rowKey: String, val value: String)
data class BackendStats(val keyCount: Int, val consistentReads: Int, val other: String)

class LoadTest(
    private val workload: Workload,
    private val keyCount: Int,
    private val clientCount: Int,
    private val backendFactory: BackendFactory
) {

    val report = mutableListOf<String>()
    val scope = CoroutineScope(Executors.newFixedThreadPool(clientCount).asCoroutineDispatcher())
    val produceScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    fun generateWorkload() {
        LOG.info("LOAD TEST: started")
        val jobs = mutableListOf<Deferred<BackendStats>>()
        val results = mutableListOf<BackendStats>()
        val sourceChannel = produceScope.generateLoad(keyCount)
        val updateTime = measureTime {
            runBlocking {
                repeat(clientCount) {
                    val backend = backendFactory(it)
                    LOG.debug("Client $it started")
                    jobs.add(runClient(it, workload, backend, sourceChannel))
                }

                results.addAll(jobs.awaitAll())
                LOG.debug("All clients finished")
            }
        }
        val consistentReads = results.sumOf { it.consistentReads }
        report.add(
            """|
            |------------------------------------------------
            |Processed $keyCount write/read cycles in $updateTime (${keyCount / updateTime.inSeconds()}) RPS
            |Reads were consistent $consistentReads times (${(consistentReads * 100.0 / keyCount).roundToInt()}%)
            |
        """.trimMargin()
        )
        results.forEach { report.add(it.other) }
        report.add("")

        LOG.info(report.joinToString("\n"))
        LOG.info("LOAD TEST: completed")
        scope.cancel()
        produceScope.cancel()
    }

    private fun runClient(
        clientNum: Int,
        workload: Workload,
        backend: Backend,
        payloadChannel: ReceiveChannel<Payload>
    ): Deferred<BackendStats> = scope.async {
        backend.use {
            when (workload) {
                Workload.MIXED -> {
                    var count = 0
                    var consistentReads = 0
                    val time = measureTime {
                        payloadChannel.consumeEach { payload ->
                            try {
                                count++
                                val firstValue = "${payload.value}_"
                                backend.put(payload.rowKey, firstValue)

                                val secondValue = payload.value
                                backend.put(payload.rowKey, secondValue)
                                val readValue = backend.get(payload.rowKey)
                                if (readValue == secondValue) {
                                    consistentReads++
                                } else {
                                    LOG.debug("Expected {} got {}", secondValue, readValue)
                                }
                            } catch (ex: Exception) {
                                LOG.error("PUT: failed to put key ${payload.rowKey} into backend $backend", ex)
                            }
                        }
                    }
                    val clientStats = """
                    Client #$clientNum processed $count write/read cycles in $time (${count / time.inSeconds()} RPS)
                """.trimIndent()
                    return@async BackendStats(keyCount = count, consistentReads = consistentReads, other = clientStats)
                }

                else -> error("Unsupported workload: $workload")
            }
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
private fun CoroutineScope.generateLoad(keyCount: Int, keyProducer: (Int) -> Int = { it }): ReceiveChannel<Payload> {
    return produce(capacity = keyCount / 2) {
        (1..keyCount).forEach {
            val key = "${keyProducer(it)}"
            val value = "val $key"
            send(Payload(key, value))
        }
        this.close()
    }
}

fun Duration.inSeconds() = this.inWholeMilliseconds / 1000.0
private val LOG = LoggerFactory.getLogger("Client.LoadTest")