package kvas.node.mapreduce

import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.Json
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasSharedProto.DataRow
import kvas.util.AnySerializer
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import java.util.concurrent.Executor
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias MapperOutput = List<Pair<String, Any?>>

interface Mapper {
    fun writeMapOutput(key: String, value: Any?)
    fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow>
}

class DemoMapperImpl: Mapper {
    override fun writeMapOutput(key: String, value: Any?) {
        TODO("Not yet implemented")
    }

    override fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow> {
        TODO("Not yet implemented")
    }

}
class MapperImpl(
    private val selfAddress: NodeAddress,
    private val storage: Storage,
    private val mapper: Mapper,
    private val executor: Executor) : MapperGrpcKt.MapperCoroutineImplBase() {
    private val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")
    private val reducerGrpcPool = GrpcPoolImpl<ReducerGrpc.ReducerBlockingStub>(selfAddress) {
            channel -> ReducerGrpc.newBlockingStub(channel)
    }

    override suspend fun startMap(request: MapperProto.StartMapRequest): MapperProto.StartMapResponse {
        val script = """
            fun mapper(rowKey: String, values: Map<String, String>): List<Pair<String, Any?>> { 
              return values[""]?.split(" ")?.map { it to 1 }?.toList() ?: emptyList()
            }
        """.trimIndent()
        scriptEngine.eval(script)
        val inv = scriptEngine as Invocable
        executor.execute {
            println("MAPPER STARTED")
            storage.scan().use { scan ->
                scan.forEach { row ->
                    val result = inv.invokeFunction("mapper", row.key, row.valuesMap)
                    (result as? MapperOutput)?.forEach { (key, value) ->
                        mapper.writeMapOutput(key, value)
                    }
                }
            }
            println("MAPPER FINISHED")
            request.metadata.shardsList.forEach { shard ->
                reducerGrpcPool.rpc(shard.leader.nodeAddress.toNodeAddress()) {
                    addMapOutputShard(addMapOutputShardRequest {
                        mapperAddress = selfAddress.toString()
                    })
                }
            }
        }
        return startMapResponse {}
    }

    override fun getMapOutputShard(request: MapperProto.GetMapOutputShardRequest): Flow<DataRow> {
        return mapper.getMapOutputShard(request.reducerAddress.toNodeAddress())
    }

    private fun writeMapOutput(key: String, value: Any?) {
        val json = Json { ignoreUnknownKeys = true }
        val jsonValue = json.encodeToString(AnySerializer, value)
        println("MAPPER OUTPUT: $key -> $jsonValue")
    }
}

class ReducerImpl(
    private val selfAddress: NodeAddress,
    private val storage: Storage): ReducerGrpcKt.ReducerCoroutineImplBase() {

    private var reduceRequest: MapperProto.StartReduceRequest = startReduceRequest {  }
    private val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")
    private val mapperGrpcPool = GrpcPoolImpl<MapperGrpc.MapperBlockingStub>(selfAddress) {
            channel -> MapperGrpc.newBlockingStub(channel)
    }

    override suspend fun startReduce(request: MapperProto.StartReduceRequest): MapperProto.StartReduceResponse {
        reduceRequest = request
        return startReduceResponse {  }
    }

    override suspend fun addMapOutputShard(request: MapperProto.AddMapOutputShardRequest): MapperProto.AddMapOutputShardResponse {
        return super.addMapOutputShard(request)
    }
}
