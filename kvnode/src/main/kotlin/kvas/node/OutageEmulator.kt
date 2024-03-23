package kvas.node

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