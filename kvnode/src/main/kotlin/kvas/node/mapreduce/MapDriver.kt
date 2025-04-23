package kvas.node.mapreduce

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
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
import kotlin.text.toInt

typealias MapperOutput = List<Pair<String, Any?>>

/**
 * Represents an interface for processing the map function output. It provides functions for:
 * - storing the map function output in a persistent storage
 * - grouping the output by the reduce key values and sharding by the reduce instance
 */
interface MapDriver {
    /**
     * Cluster metadata information.
     */
    var metadata: ClusterMetadata

    /**
     * Called once per every pair of (key, value) output of the mapper function.
     * It may be called many times with the same key and value, and such invocations and pairs shall be considered
     * as different.
     *
     * The value type is declared as Any, but in reality it is expected to be a primitive type,
     * a list, a map or an object constructed from them. It is expected to be JSON-serializeable.
     *
     * The driver may, but doesn't have to, apply reduce key sharding and append the sharding data
     * to the key-value record.
     */
    fun writeMapOutput(key: String, value: Any?)

    /**
     * Returns a map output shard: all map output pairs for the reduce shard associated with the given address.
     * The pairs are encoded as DataRow objects where a row key is a map output key and a default column
     * value is a map output value.
     */
    fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow>
}

typealias MapDriverFactory = (Storage, Sharding) -> MapDriver
typealias MapDriverProvider = Pair<String, MapDriverFactory>

object MapDrivers {
    val DEMO: MapDriverProvider = "demo" to ::DemoMapDriver
    val REAL: MapDriverProvider = "real" to { _: Storage, _: Sharding ->
        TODO("Task 8: Create your real reduce driver here")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

private typealias GrpcAddMapOutputShard = (NodeAddress, MapperProto.AddMapOutputShardRequest)->MapperProto.AddMapOutputShardResponse

/**
 * Implementation of a gRPC service that runs map function over the entire contents of the storage and
 * makes map output shards with the help of a map driver.
 */
class MapperImpl(
    private val selfAddress: NodeAddress,
    /** The data storage */
    private val storage: Storage,
    private val mapDriver: MapDriver,
    /** Executor to run the map process */
    private val executor: Executor,
    /** A function that sends a blocking AddMapOutputShard gRPC request to  the given reducer service */
    private val grpcAddMapOutputShard: GrpcAddMapOutputShard? = null,
    private val sharedStorage: ()->Storage,
    ) : MapperGrpcKt.MapperCoroutineImplBase() {
    private val reducerGrpcPool: GrpcPool<ReducerGrpc.ReducerBlockingStub> =
        GrpcPoolImpl<ReducerGrpc.ReducerBlockingStub>(selfAddress) { channel ->
            ReducerGrpc.newBlockingStub(channel)
        }

    override suspend fun startMap(request: MapperProto.StartMapRequest): MapperProto.StartMapResponse = try {
        val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")
        mapDriver.metadata = request.metadata
        val script = request.mapFunction
        scriptEngine.eval(script)
        val inv = scriptEngine as Invocable
        executor.execute {
            LOG.info("MAPPER STARTED")
            storage.scan().use { scan ->
                scan.forEach { row ->
                    val result = inv.invokeFunction("mapper", row.key, row.valuesMap, this.sharedStorage.invoke())
                    (result as? MapperOutput)?.forEach { (key, value) ->
                        mapDriver.writeMapOutput(key, value)
                    }
                }
            }
            LOG.info("MAPPER FINISHED")
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

/**
 * This is a demo map driver. It makes a single map output shard for all reducers.
 * The values emitted from the map function are encoded in the map output shard as JSON objects.
 */
class DemoMapDriver(private val storage: Storage, private val sharding: Sharding): MapDriver {
    override var metadata: ClusterMetadata = clusterMetadata { }

    override fun writeMapOutput(key: String, value: Any?) {
        val json = Json { ignoreUnknownKeys = true }
        val jsonValue = json.encodeToString(AnySerializer, value)
        val valueCount = storage.get(key, "valueCount")?.toInt() ?: 0
        storage.put(key, "valueCount", (valueCount + 1).toString())
        storage.put(key, "value$valueCount", jsonValue)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun getMapOutputShard(reducerAddress: NodeAddress): Flow<DataRow> {
        return storage.scan().use {
            it.asFlow().flatMapConcat {
                it.valuesMap.entries.filter {it.key != "valueCount"}.asFlow().map { v ->
                    DataRow.newBuilder().setKey(it.key).putValues(DEFAULT_COLUMN_NAME, v.value).build()
                }
            }
        }
    }
}


private val LOG = LoggerFactory.getLogger("MapReduce.Map")