package kvas.node.mapreduce

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kvas.node.storage.Storage
import kvas.proto.MapperGrpcKt
import kvas.proto.MapperProto
import kvas.proto.startMapResponse
import kvas.util.AnySerializer
import java.util.concurrent.Executor
import javax.script.Invocable
import javax.script.ScriptEngineManager

typealias MapperOutput = List<Pair<String, Any?>>

class MapperImpl(private val storage: Storage, private val executor: Executor) : MapperGrpcKt.MapperCoroutineImplBase() {
    private val scriptEngine = ScriptEngineManager().getEngineByExtension("kts")

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
                        writeMapOutput(key, value)
                    }
                }
            }
        }
        return startMapResponse {}
    }

    private fun writeMapOutput(key: String, value: Any?) {
        val json = Json { ignoreUnknownKeys = true }
        val jsonValue = json.encodeToString(AnySerializer, value)
        println("MAPPER OUTPUT: $key -> $jsonValue")
    }
}