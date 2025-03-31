package kvas.node.mapreduce

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.json.Json
import kvas.node.storage.DEFAULT_COLUMN_NAME
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.proto.KvasSharedProto.DataRow
import kvas.setup.Sharding
import kvas.util.AnySerializer
import kvas.util.GrpcPool
import kvas.util.GrpcPoolImpl
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory
import java.util.concurrent.Executor
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias MapperOutput = List<Pair<String, Any?>>

interface MapDriver {
    var metadata: ClusterMetadata
    fun writeMapOutput(key: String, value: Any?)
    fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow>
}

class DemoMapDriver(private val storage: Storage, private val sharding: Sharding): MapDriver {
    override var metadata: ClusterMetadata = clusterMetadata { }

    override fun writeMapOutput(key: String, value: Any?) {
        val json = Json { ignoreUnknownKeys = true }
        val jsonValue = json.encodeToString(AnySerializer, value)
        storage.put(key, columnName = DEFAULT_COLUMN_NAME, jsonValue)
    }

    override fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow> {
        return storage.scan().use {
            it.asFlow().map { DataRow.newBuilder().setKey(it.key).putAllValues(it.valuesMap).build() }
        }
    }
}

typealias MapDriverFactory = (Storage, Sharding) -> MapDriver
typealias MapDriverProvider = Pair<String, MapDriverFactory>

object MapDrivers {
    val DEMO: MapDriverProvider = "demo" to ::DemoMapDriver
    val REAL: MapDriverProvider = "real" to { _: Storage, _: Sharding ->
        TODO("Create your real reduce driver here")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

class MapperImpl(
    private val selfAddress: NodeAddress,
    private val storage: Storage,
    private val mapDriver: MapDriver,
    private val executor: Executor,
    private val grpcAddMapOutputShard: ((NodeAddress, MapperProto.AddMapOutputShardRequest)-> MapperProto.AddMapOutputShardResponse)? = null

    ) : MapperGrpcKt.MapperCoroutineImplBase() {
    private val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")
    private val reducerGrpcPool: GrpcPool<ReducerGrpc.ReducerBlockingStub> =
        GrpcPoolImpl<ReducerGrpc.ReducerBlockingStub>(selfAddress) { channel ->
            ReducerGrpc.newBlockingStub(channel)
        }

    override suspend fun startMap(request: MapperProto.StartMapRequest): MapperProto.StartMapResponse = try {
        mapDriver.metadata = request.metadata
        val script = request.mapFunction
        scriptEngine.eval(script)
        val inv = scriptEngine as Invocable
        executor.execute {
            println("MAPPER STARTED")
            storage.scan().use { scan ->
                scan.forEach { row ->
                    val result = inv.invokeFunction("mapper", row.key, row.valuesMap)
                    (result as? MapperOutput)?.forEach { (key, value) ->
                        mapDriver.writeMapOutput(key, value)
                    }
                }
            }
            println("MAPPER FINISHED")
            request.metadata.shardsList.forEach { shard ->
                val leaderAddress = shard.leader.nodeAddress.toNodeAddress()
                val request = addMapOutputShardRequest {
                    mapperAddress = selfAddress.toString()
                }
                grpcAddMapOutputShard?.invoke(leaderAddress, request)
                    ?: reducerGrpcPool.rpc(shard.leader.nodeAddress.toNodeAddress()) {
                        addMapOutputShard(request)
                    }
            }
        }
        startMapResponse {}
    } catch (ex: Exception) {
        LOG.error("""Failed to start map
            |{}
        """.trimMargin(), request, ex)
        throw ex
    }

    override fun getMapOutputShard(request: MapperProto.GetMapOutputShardRequest): Flow<DataRow> {
        return mapDriver.getMapOutputShard(request.reducerAddress.toNodeAddress())
    }
}

private val LOG = LoggerFactory.getLogger("Node.Map")