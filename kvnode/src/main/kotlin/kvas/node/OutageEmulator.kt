package kvas.node

import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import kotlinx.coroutines.runBlocking
import java.lang.RuntimeException
import kotlin.random.Random

/**
 * This class generates sequences of successful and failed requests.
 * The lengths of the sequences are random centered around the provided mean values
 * The sequences are interleaving, that is, a sequence of failed requests is followed by a sequence of successful,
 * followed by a sequence of failed, and so on.
 */
class OutageEmulator<T>(private val meanRequestsToFail: Int = 50, private val meanRequestsToRecover: Int = 5) {
  private var servedRequests = 0
  private var failedRequests = 0
  private var countDown = 0
  private var willServe = true
  var disabled: Boolean = false

  init {
    countDown = nextCountDown()
  }

  private fun nextCountDown() = if (willServe) generateRequestsToFail() else generateRequestsToRecover()
  private fun generateRequestsToFail() = meanRequestsToFail/4 + Random.nextInt(meanRequestsToFail/2)
  private fun generateRequestsToRecover() = meanRequestsToRecover/4 + Random.nextInt(meanRequestsToRecover/2)
  fun serveIfAvailable(code: ()->T): Result<T> {
    val result: Result<T> = if (disabled) {
      Result.success(code()).also { servedRequests++ }
    } else run {
      if (willServe) Result.success(code()).also { servedRequests++ }
      else Result.failure<T>(OutageEmulatorException("Service unavailable. Will recover in $countDown requests")).also { failedRequests++ }
    }.also {
      if (--countDown == 0) {
        willServe = !willServe
        countDown = nextCountDown()
      }
    }
    return result
  }
}

class OutageEmulatorException(msg: String): RuntimeException(msg)

enum class OutageMode {
  UNAVAILABLE, AVAILABLE, SERIES
}
data class OutageConfig(
  var counter: Int = 0,
  var mode: OutageMode = OutageMode.AVAILABLE,
  val nextCountDown: (config: OutageConfig)->Int = {0},
  var willServe: Boolean = true) {

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

fun createSeriesCountDown(meanRequestsToFail: Int, meanRequestsToRecover: Int): (OutageConfig)->Int =
  {
    if (it.willServe)
      meanRequestsToFail/4 + Random.nextInt(meanRequestsToFail/2)
    else
      meanRequestsToRecover/4 + Random.nextInt(meanRequestsToRecover/2)
  }

suspend fun <T> whenAvailable1(config: OutageConfig, code: suspend ()->T): com.github.michaelbull.result.Result<T, OutageEmulatorException> =
  whenAvailable(config) {
    runBlocking { code() }
  }
fun <T> whenAvailable(config: OutageConfig, code: ()->T): com.github.michaelbull.result.Result<T, OutageEmulatorException> =
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
