package kvas.node.storage

import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import kotlinx.coroutines.runBlocking
import kotlin.math.max
import kotlin.random.Random

/**
 * This class generates sequences of successful and failed requests.
 * The lengths of the sequences are random centered around the provided mean values
 * The sequences are interleaving, that is, a sequence of failed requests is followed by a sequence of successful,
 * followed by a sequence of failed, and so on.
 */
class OutageEmulator(val meanRequestsToFail: Int = 50, val meanRequestsToRecover: Int = 5) {
    internal var servedRequests = 0
    internal var failedRequests = 0
    internal var countDown = 0
    internal var willServe = true
    var disabled: Boolean = false

    init {
        countDown = nextCountDown()
    }

    internal fun nextCountDown() = if (willServe) generateRequestsToFail() else generateRequestsToRecover()
    private fun generateRequestsToFail() =
        max(1, meanRequestsToFail / 4 + Random.nextInt(max(1, meanRequestsToFail / 2)))

    private fun generateRequestsToRecover() =
        max(1, meanRequestsToRecover / 4 + Random.nextInt(max(1, meanRequestsToRecover / 2)))
}

fun <T> OutageEmulator.serveIfAvailable(code: () -> T): com.github.michaelbull.result.Result<T, String> = runBlocking {
    synchronized(this) {
        val result = if (disabled) {
            servedRequests++
            Ok(code())
        } else run {
            servedRequests++
            if (willServe) Ok(code())
            else {
                failedRequests++
                Err("Service unavailable. Will recover in $countDown requests")
            }
        }
        if (--countDown <= 0) {
            willServe = !willServe
            countDown = nextCountDown()
        }
        result
    }
}


class OutageEmulatorException(msg: String) : RuntimeException(msg)

enum class OutageMode {
    UNAVAILABLE, AVAILABLE, SERIES
}

data class OutageConfig(
    var counter: Int = 0,
    var mode: OutageMode = OutageMode.AVAILABLE,
    val nextCountDown: (config: OutageConfig) -> Int = { 0 },
    var willServe: Boolean = true
) {

    init {
        counter = nextCountDown(this)
    }

    fun countDown() {
        if (--counter == 0) {
            willServe = !willServe
            counter = nextCountDown(this)
        }
    }
}

fun createSeriesCountDown(meanRequestsToFail: Int, meanRequestsToRecover: Int): (OutageConfig) -> Int =
    {
        if (it.willServe)
            meanRequestsToFail / 4 + Random.nextInt(meanRequestsToFail / 2)
        else
            meanRequestsToRecover / 4 + Random.nextInt(meanRequestsToRecover / 2)
    }

suspend fun <T> whenAvailable1(
    config: OutageConfig,
    code: suspend () -> T
): com.github.michaelbull.result.Result<T, OutageEmulatorException> =
    whenAvailable(config) {
        runBlocking { code() }
    }

fun <T> whenAvailable(
    config: OutageConfig,
    code: () -> T
): com.github.michaelbull.result.Result<T, OutageEmulatorException> =
    when (config.mode) {
        OutageMode.UNAVAILABLE -> Err(OutageEmulatorException("Service Unavailable"))
        OutageMode.AVAILABLE -> Ok(code())
        OutageMode.SERIES -> run {
            if (config.willServe) Ok(code())
            else Err(OutageEmulatorException("Service unavailable. Will recover in ${config.counter} requests"))
        }.also {
            config.countDown()
        }
    }
