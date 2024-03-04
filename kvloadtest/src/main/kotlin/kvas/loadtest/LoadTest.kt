package kvas.loadtest

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

interface Backend {
  val stats: String

  fun put(key: String, value: String)
  fun get(key: String): String?
}

typealias KV = Pair<String, String>
private data class KvasNodeJob(val receivingChannel: Channel<KV>, val job: Job, val backend: Backend)

private fun createPutJob(
  partitionNumber: Int, backend: Backend, scope: CoroutineScope
): KvasNodeJob {
  val channel = Channel<KV>(Channel.UNLIMITED)
  val job = scope.launch {
    sendPutRequests(channel, partitionNumber, backend)
  }
  return KvasNodeJob(channel, job, backend)
}

@OptIn(ExperimentalTime::class)
private fun createGetJob(
  partitionNumber: Int, backend: Backend, scope: CoroutineScope
): KvasNodeJob {
  val channel = Channel<KV>(Channel.UNLIMITED)
  val job = scope.launch {
    sendGetRequests(channel, partitionNumber, backend)
  }
  return KvasNodeJob(channel, job, backend)
}

private suspend fun sendPutRequests(channel: Channel<KV>, jobNumber: Int, backend: Backend) {
  println("Started PUT job #$jobNumber in ${Thread.currentThread()}")
  var count = 0
  val time = measureTime {
    for ((key, value) in channel) {
      try {
        backend.put(key, value)
        count++
      } catch (ex: Exception) {
        ex.printStackTrace()
        exitProcess(1)
      }
    }
  }
  println("Finished PUT job #$jobNumber. Processed $count requests in $time (${count/time.inSeconds()} RPS)")
}

@ExperimentalTime
private suspend fun sendGetRequests(
  channel: Channel<KV>,
  jobNumber: Int,
  backend: Backend) {

  println("Started GET job #$jobNumber in ${Thread.currentThread()}")
  var count = 0
  val time = measureTime {
    for ((key, value) in channel) {
      backend.get(key)?.let {
        if (it == value) {
          count++
        } else {
          println("GET: Unexpected response for key=$key. Expected value=$value, got $it")
        }
      } ?: run {
        println("GET: Unexpected NO response for key=$key")
      }
    }
  }
  println("Finished GET job #$jobNumber. Processed $count requests in $time (${count/time.inSeconds()} RPS)")
}

private suspend fun sendPutGetRequests(channel: Channel<KV>, jobNum: Int, backend: Backend): Int {
  println("Started MIXED job #$jobNum in ${Thread.currentThread()}")
  var count = 0
  var consistentReads = 0
  val time = measureTime {
    for ((key, value) in channel) {
      try {
        count++
        val firstValue = "${value}_"
        backend.put(key, firstValue)

        val secondValue = value
        backend.put(key, secondValue)
        val readValue = backend.get(key)
        if (readValue == secondValue) {
          consistentReads++
        } else {
          println("Expected $secondValue got $readValue")
        }
      } catch (ex: Exception) {
        ex.printStackTrace()
        println("Failed to PUT key $key into backend $backend")
      }
    }
  }
  println("Finished MIXED job #$jobNum. Processed $count requests in $time (${count/time.inSeconds()} RPC)")
  println("Reads were consistent $consistentReads times")
  return consistentReads
}
private suspend fun generateLoad(channels: Map<Int, Channel<KV>>, keyCount: Int, keyProducer: (Int)->Int = {it}) {
  for (idx in 0 until keyCount) {
    val key = "${keyProducer(idx)}"
    val value = "val $key"
    val partitionNumber = key.hashCode() % channels.size
    channels[partitionNumber]?.let {
      it.send(key to value)
    }?: run {
      println("Channel $partitionNumber not found")
      exitProcess(1)
    }
  }

  for (channel in channels.values) {
    channel.close()
  }
}

enum class Workload {
  MIXED, READONLY, SEQUENTIAL
}
typealias BackendFactory = (Int)->Backend
@ExperimentalTime
suspend fun runTest(workload: Workload, keyCount: Int, channelCount: Int, backendFactory: BackendFactory): String {
  val report = mutableListOf<String>()
  val scope = CoroutineScope( Executors.newFixedThreadPool(channelCount).asCoroutineDispatcher())

  suspend fun generatePutWorkload() {
    println("Generating PUT requests")
    val loaders = (1..channelCount).map {
      createPutJob(it, backendFactory(it), scope)
    }
    val putChannels = loaders.mapIndexed { index, job -> index to job.receivingChannel }.toMap()
    val putTime = measureTime {
      generateLoad(putChannels, keyCount)
      loaders.map { it.job }.forEach { it.join() }
    }
    report.add("Processed $keyCount PUT requests in $putTime (${keyCount / putTime.inSeconds()} RPS)")
    report.add("Backend stats:")
    report.addAll(loaders.map { it.backend.stats }.toList())
  }

  suspend fun generateUpdateWorkload() {
    val updaters = (1..channelCount).map {
      createPutJob(it, backendFactory(it), scope)
    }
    val updateChannels = updaters.mapIndexed { index, job -> index to job.receivingChannel }.toMap()
    val updateTime = measureTime {
      generateLoad(updateChannels, keyCount) { Random.nextInt(keyCount / 2) }
      updaters.map { it.job }.forEach { it.join() }
    }
    report.add("Processed $keyCount UPDATE requests in $updateTime (${keyCount / updateTime.inSeconds()} RPS)")
    report.add("Backend stats:")
    report.addAll(updaters.map { it.backend.stats }.toList())
  }

  suspend fun generateGetWorkload() {
    println("Generating GET requests")
    val readers = (1..channelCount).map {
      createGetJob(it, backendFactory(it), scope)
    }
    val readChannels = readers.mapIndexed { index, job ->  index to job.receivingChannel }.toMap()
    val getTime = measureTime {
      generateLoad(readChannels, keyCount)
      readers.map { it.job }.forEach { it.join() }
    }
    report.add("Processed $keyCount GET requests in $getTime (${keyCount/getTime.inSeconds()} RPS)")
    report.add("Backend stats:")
    report.addAll(readers.map { it.backend.stats }.toList())

  }

  suspend fun generatePutGetWorkload() {
    println("Generating PUT and GET requests")
    var consistentReads = AtomicInteger(0)
    val jobs = (1..channelCount).map {
      val backend = backendFactory(it)
      val channel = Channel<KV>(Channel.UNLIMITED)
      val job = scope.launch {
        consistentReads.getAndAdd(sendPutGetRequests(channel, it, backend))
      }
      KvasNodeJob(channel, job, backend)
    }
    val channels = jobs.mapIndexed { idx, job -> idx to job.receivingChannel }.toMap()
    val updateTime = measureTime {
      generateLoad(channels, keyCount)
      jobs.map { it.job }.forEach { it.join() }
    }
    report.add("Processed $keyCount write/read cycles in $updateTime (${keyCount/updateTime.inSeconds()}) RPS")
    report.add("Reads were consistent ${consistentReads.get()} times (${(consistentReads.get()*100.0/keyCount).roundToInt()}%)")
    report.add("Backend stats:")
    report.addAll(jobs.map { it.backend.stats }.toList())
  }

  when (workload) {
    Workload.MIXED -> {
      generatePutGetWorkload()
    }
    Workload.READONLY -> {
      generateGetWorkload()
    }
    Workload.SEQUENTIAL -> {
      generatePutWorkload()
      generateUpdateWorkload()
      generateGetWorkload()
    }
  }

  return report.joinToString(separator = "\n")
}

fun Duration.inSeconds() = this.inWholeMilliseconds/1000.0