package kvas.node.mapreduce

import kotlinx.serialization.json.Json
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.setup.Sharding
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import java.util.concurrent.Executor
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias ReduceTaskFn = (String, List<String>)->Unit

interface ReduceDriver {
    fun writeReduceInput(key: String, value: String)
    fun writeReduceOutput(key: String, value: Any)

    fun forEachReduceTask(reduceTaskFn: ReduceTaskFn)
}

object ReduceDrivers {
    val DEMO = "demo" to ::DemoReduceDriver
    val REAL = "real" to { _: Storage, _: Storage ->
        TODO("Create your real reduce driver here")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

class DemoReduceDriver(private val inputStorage: Storage, private val outputStorage: Storage): ReduceDriver {
    override fun writeReduceInput(key: String, value: String) {
        val valueCount = inputStorage.get(key, "valueCount")?.toInt() ?: 0
        inputStorage.put(key, "valueCount", (valueCount + 1).toString())
        inputStorage.put("$key:$valueCount", DEFAULT_COLUMN_NAME, value)
    }

    override fun writeReduceOutput(key: String, value: Any) {
        println("writeReduceOutput($key, $value)")
    }

    override fun forEachReduceTask(reduceTaskFn: ReduceTaskFn) {
        inputStorage.scan().use { scan ->
            var key: String? = null
            var counter = 0
            var totalValues = 0
            val values = mutableListOf<String>()
            scan.forEach { row ->
                if (key == null) {
                    key = row.key
                    totalValues = row.valuesMap["valueCount"]?.toInt() ?: error("I expect the value count to be set")
                } else if ("$key:$counter" == row.key) {
                    values.add(row.valuesMap[DEFAULT_COLUMN_NAME] ?: "")
                    counter++
                } else {
                    if (counter != totalValues) {
                        error("Expected: counter==$totalValues, actual: $counter=$counter, totalValues=$totalValues, key=$key, current row: $row")
                    }
                    reduceTaskFn(key!!, values)
                }
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
            mapperGrpcPool.rpc(request.mapperAddress.toNodeAddress()) {
                getMapOutputShard(getMapOutputShardRequest {
                    this.reducerAddress = selfAddress.toString()
                }).forEach { dataRow ->
                    reduceDriver.writeReduceInput(dataRow.key, dataRow.valuesMap[DEFAULT_COLUMN_NAME] ?: "")
                }
            }
            availableMapOutputShards.add(request.mapperAddress)
            if (availableMapOutputShards.size == reduceRequest.metadata.shardsCount) {
                executeReduce()
            }
        }
        return addMapOutputShardResponse {  }
    }

    private fun executeReduce() {
        val json = Json { ignoreUnknownKeys = true }
        reduceDriver.forEachReduceTask { key, values ->
            val jsonValues = values.map { json.parseToJsonElement(it) }.toList()
            scriptEngine.eval(reduceRequest.reduceFunction)
            val inv = scriptEngine as Invocable
            val result = inv.invokeFunction("reducer", key, jsonValues)
            reduceDriver.writeReduceOutput(key, result)
        }
    }
}
