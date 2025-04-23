package kvas.node.mapreduce

import kotlinx.serialization.json.Json
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias ReduceTaskFn = (String, List<String>)->Unit

/**
 * Represents an object that is responsible for:
 * - building the reduce tasks from the map output shards
 * - executing the reduce function for every reduce shard
 * - writing the reduce output to the storage
 */
interface ReduceDriver {
    /**
     * Writes a key-value pair from a map output shard to the reduce input shard.
     */
    fun writeReduceInput(key: String, value: String)

    /**
     * Processes the output produced by the reduce function call. It may be stored persistently,
     * or printed, or handled any other legal way.
     */
    fun writeReduceOutput(key: String, value: Any)

    /**
     * This function will be called when all map output shards from all mappers have been collected.
     * The function must group all values with the same reduce key and feed pairs of
     * key and associated values to the reduce function one by one.
     */
    fun forEachReduceTask(reduceTaskFn: ReduceTaskFn)
}

typealias ReduceDriverFactory = (Storage, Storage) -> ReduceDriver
typealias ReduceDriverProvider = Pair<String, ReduceDriverFactory>

object ReduceDrivers {
    val DEMO: ReduceDriverProvider = "demo" to ::DemoReduceDriver
    val REAL: ReduceDriverProvider = "real" to { _: Storage, _: Storage ->
        TODO("Task 8: Create your real reduce driver here")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

/**
 * Implementation of a gRPC service that collects map output shards from the mappers and runs reduce
 * once all mappers have been processed.
 */
class ReducerImpl(
    private val selfAddress: NodeAddress,
    private val sharedStorage: ()->Storage,
    private val reduceDriver: ReduceDriver,
    private val shardTransferExecutor: Executor
): ReducerGrpcKt.ReducerCoroutineImplBase() {

    private val reduceExecutor = Executors.newSingleThreadExecutor()
    private var reduceRequest: MapperProto.StartReduceRequest = startReduceRequest {  }
    private val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")
    private val mapperGrpcPool = GrpcPoolImpl<MapperGrpc.MapperBlockingStub>(selfAddress) {
            channel -> MapperGrpc.newBlockingStub(channel)
    }
    private val availableMapOutputShards = mutableSetOf<String>()

    override suspend fun startReduce(request: MapperProto.StartReduceRequest): MapperProto.StartReduceResponse {
        reduceRequest = request
        return startReduceResponse {  }
    }

    override suspend fun addMapOutputShard(request: MapperProto.AddMapOutputShardRequest): MapperProto.AddMapOutputShardResponse {
        shardTransferExecutor.execute {
            LOG.info("Transferring map output from ${request.mapperAddress}")
            mapperGrpcPool.rpc(request.mapperAddress.toNodeAddress()) {
                getMapOutputShard(getMapOutputShardRequest {
                    this.reducerAddress = selfAddress.toString()
                }).forEach { dataRow ->
                    reduceDriver.writeReduceInput(dataRow.key, dataRow.valuesMap[DEFAULT_COLUMN_NAME] ?: "")
                }
            }
            LOG.info("...done")
            availableMapOutputShards.add(request.mapperAddress)
            if (availableMapOutputShards.size == reduceRequest.metadata.shardsCount) {
                executeReduce()
            }
        }
        return addMapOutputShardResponse {  }
    }

    private fun executeReduce() {
        reduceExecutor.execute {
            LOG.info("REDUCE STARTED")
            scriptEngine.eval(reduceRequest.reduceFunction)
            val inv = scriptEngine as Invocable
            val json = Json { ignoreUnknownKeys = true }
            reduceDriver.forEachReduceTask { key, values ->
                val jsonValues = values.map { json.parseToJsonElement(it) }.toList()
                val result = inv.invokeFunction("reducer", key, jsonValues, this.sharedStorage.invoke())
                reduceDriver.writeReduceOutput(key, result)
            }
            LOG.info("REDUCE FINISHED")
        }
    }
}

/**
 * This is a demo reduce driver. It relies on the capabilities of the underlying storage, such as
 * the capability to have arbitrary many columns in a single row.
 *
 * All values with the same reduce key are stored in the columns named value0, value1, ..., valueN
 * The total number of values with the same reduce key is stored in a column with "valueCount" name.
 */
class DemoReduceDriver(private val inputStorage: Storage, private val outputStorage: Storage): ReduceDriver {
    /**
     * Increments the valueCount column for the given key and writes the value to the column
     * named "value${valueCount}"
     */
    override fun writeReduceInput(key: String, value: String) {
        val valueCount = inputStorage.get(key, "valueCount")?.toInt() ?: 0
        inputStorage.put(key, "valueCount", (valueCount + 1).toString())
        inputStorage.put(key, "value$valueCount", value)
    }

    /**
     * Writes the reduce output to the output storage.
     */
    override fun writeReduceOutput(key: String, value: Any) {
        outputStorage.put(key, DEFAULT_COLUMN_NAME, "$value")
    }

    /**
     * Scans through the reduce input storage and builds a list of values from the
     * valueN columns. Prints the output storage contents at the end of scanning.
     */
    override fun forEachReduceTask(reduceTaskFn: ReduceTaskFn) {
        inputStorage.scan().use { scan ->
            scan.forEach { row ->
                val values = mutableListOf<String>()
                var totalValues = row.valuesMap["valueCount"]?.toInt() ?: error("I expect the value count to be set. row=$row")
                (0 until totalValues).forEach {
                    values.add(row.valuesMap["value$it"] ?: error("I expect the value to be set"))
                }
                // The reduce function output will be fed to writeReduceOutput.
                reduceTaskFn(row.key, values)
            }
        }
        LOG.info("REDUCE OUTPUT:")
        outputStorage.scan().use { scan ->
            scan.forEach { row ->
                LOG.info("${row.key} -> ${row.valuesMap[DEFAULT_COLUMN_NAME]}")
            }
        }
    }
}

private val LOG = LoggerFactory.getLogger("MapReduce.Reduce")