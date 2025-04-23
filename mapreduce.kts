import kotlin.collections.*
import kotlinx.serialization.json.*
import kvas.node.storage.Storage

fun mapper(rowKey: String, values: Map<String, String>, sharedStorage: Storage): List<Pair<String, Any?>> {
  return values[""]?.split(" ")?.map { it to 1 }?.toList() ?: emptyList()
}

fun reducer(reduceKey: String, values: List<Any>, sharedStorage: Storage): Any {
  return values.sumOf { (it as JsonElement).jsonPrimitive.int }
}
