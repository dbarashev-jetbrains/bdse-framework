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
  var servedRequests = 0
  var failedRequests = 0
  var countDown = 0
  var isAvailable = true

  init {
    countDown = nextCountDown()
  }

  private fun nextCountDown() = if (isAvailable) generateRequestsToFail() else generateRequestsToRecover()
  private fun generateRequestsToFail() = meanRequestsToFail/4 + Random.nextInt(meanRequestsToFail/2)
  private fun generateRequestsToRecover() = meanRequestsToRecover/4 + Random.nextInt(meanRequestsToRecover/2)
  fun serveIfAvailable(code: ()->T): Result<T> {
    val result: Result<T> = if (isAvailable) Result.success(code()).also { servedRequests++ }
    else Result.failure<T>(RuntimeException("Service unavailable")).also { failedRequests++ }
    if (--countDown == 0) {
      isAvailable = !isAvailable
      countDown = nextCountDown()
    }
    return result
  }
}