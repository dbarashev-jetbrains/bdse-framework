package kvas.node.mapreduce

import kotlinx.serialization.json.Json
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias ReduceTaskFn = (String, List<String>)->Unit

interface ReduceDriver {
    fun writeReduceInput(key: String, value: String)
    fun writeReduceOutput(key: String, value: Any)

    fun forEachReduceTask(reduceTaskFn: ReduceTaskFn)
}

typealias ReduceDriverFactory = (Storage, Storage) -> ReduceDriver
typealias ReduceDriverProvider = Pair<String, ReduceDriverFactory>
object ReduceDrivers {
    val DEMO: ReduceDriverProvider = "demo" to ::DemoReduceDriver
    val REAL: ReduceDriverProvider = "real" to { _: Storage, _: Storage ->
        TODO("Create your real reduce driver here")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

class DemoReduceDriver(private val inputStorage: Storage, private val outputStorage: Storage): ReduceDriver {
    override fun writeReduceInput(key: String, value: String) {
        val valueCount = inputStorage.get(key, "valueCount")?.toInt() ?: 0
        inputStorage.put(key, "valueCount", (valueCount + 1).toString())
        inputStorage.put(key, "value$valueCount", value)
    }

    override fun writeReduceOutput(key: String, value: Any) {
        outputStorage.put(key, DEFAULT_COLUMN_NAME, "$value")
    }

    override fun forEachReduceTask(reduceTaskFn: ReduceTaskFn) {
        inputStorage.scan().use { scan ->
            scan.forEach { row ->
                val values = mutableListOf<String>()
                var totalValues = row.valuesMap["valueCount"]?.toInt() ?: error("I expect the value count to be set. row=$row")
                (0 until totalValues).forEach {
                    values.add(row.valuesMap["value$it"] ?: error("I expect the value to be set"))
                }
                reduceTaskFn(row.key, values)
            }
        }
        println("REDUCE OUTPUT:")
        outputStorage.scan().use { scan ->
            scan.forEach { row ->
                println("${row.key} -> ${row.valuesMap[DEFAULT_COLUMN_NAME]}")
            }
        }
    }
}

class ReducerImpl(
    private val selfAddress: NodeAddress,
    private val storage: Storage,
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
            println("Transferring map output from ${request.mapperAddress}")
            mapperGrpcPool.rpc(request.mapperAddress.toNodeAddress()) {
                getMapOutputShard(getMapOutputShardRequest {
                    this.reducerAddress = selfAddress.toString()
                }).forEach { dataRow ->
                    reduceDriver.writeReduceInput(dataRow.key, dataRow.valuesMap[DEFAULT_COLUMN_NAME] ?: "")
                }
            }
            println("...done")
            availableMapOutputShards.add(request.mapperAddress)
            if (availableMapOutputShards.size == reduceRequest.metadata.shardsCount) {
                executeReduce()
            }
        }
        return addMapOutputShardResponse {  }
    }

    private fun executeReduce() {
        reduceExecutor.execute {
            println("REDUCE STARTED")
            scriptEngine.eval(reduceRequest.reduceFunction)
            val inv = scriptEngine as Invocable
            val json = Json { ignoreUnknownKeys = true }
            reduceDriver.forEachReduceTask { key, values ->
                val jsonValues = values.map { json.parseToJsonElement(it) }.toList()
                val result = inv.invokeFunction("reducer", key, jsonValues)
                reduceDriver.writeReduceOutput(key, result)
            }
            println("REDUCE FINISHED")
        }
    }
}
