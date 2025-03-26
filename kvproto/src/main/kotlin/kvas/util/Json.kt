package kvas.util

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.json.*
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.temporal.TemporalAccessor

object AnySerializer : KSerializer<Any?> {
    override val descriptor: SerialDescriptor = JsonElement.serializer().descriptor
    override fun deserialize(decoder: Decoder): Any? {
        if (decoder is JsonDecoder) {
            val jsonObject = decoder.decodeJsonElement()
            return jsonObject.toPrimitive()
        } else {
            throw NotImplementedError("Decoder $decoder is not supported!")
        }
    }

    override fun serialize(encoder: Encoder, value: Any?) {
        if (encoder is JsonEncoder) {
            encoder.encodeJsonElement(value.toJsonElement())
        } else {
            throw NotImplementedError("Encoder $encoder is not supported!")
        }
    }
}
object MapAnySerializer : KSerializer<Map<String, Any?>> {
    @Serializable
    private abstract class MapAnyMap : Map<String, Any?>

    override val descriptor: SerialDescriptor = MapAnyMap.serializer().descriptor

    override fun deserialize(decoder: Decoder): Map<String, Any?> {
        if (decoder is JsonDecoder) {
            val jsonObject = decoder.decodeJsonElement() as JsonObject
            return jsonObject.toPrimitiveMap()
        } else {
            throw NotImplementedError("Decoder $decoder is not supported!")
        }
    }

    override fun serialize(encoder: Encoder, value: Map<String, Any?>) {
        if (encoder is JsonEncoder) {
            encoder.encodeJsonElement(value.toJsonElement())
        } else {
            throw NotImplementedError("Encoder $encoder is not supported!")
        }
    }

}


fun Any?.toJsonElement(): JsonElement = when (this) {
    null -> JsonNull
    is JsonElement -> this
    is Number -> JsonPrimitive(this)
    is String -> JsonPrimitive(this)
    is Boolean -> JsonPrimitive(this)
    is Enum<*> -> JsonPrimitive(this.toString())
    is TemporalAccessor -> JsonPrimitive(this.toString())
    is Array<*> -> this.toJsonElement()
    is Iterable<*> -> this.toJsonElement()
    is Map<*, *> -> this.toJsonElement()
    else -> throw IllegalStateException("Can't serialize unknown type: $this")
}

fun Iterable<*>.toJsonElement() =
    JsonArray(this.map { it.toJsonElement() })

fun Array<*>.toJsonElement() =
    JsonArray(this.map { it.toJsonElement() })

fun Map<*, *>.toJsonElement() =
    JsonObject(this.map { (key, value) -> key as String to value.toJsonElement() }.toMap())

fun JsonElement.toPrimitive(): Any? = when (this) {
    is JsonNull -> null
    is JsonObject -> this.toPrimitiveMap()
    is JsonArray -> this.toPrimitiveList()
    is JsonPrimitive -> {
        if (isString) {
            contentOrNull
        } else {
            booleanOrNull ?: longOrNull ?: doubleOrNull
        }
    }
    else -> null
}

fun JsonObject.toPrimitiveMap(): Map<String, Any?> =
    this.map { (key, value) -> key to value.toPrimitive() }.toMap()

fun JsonArray.toPrimitiveList(): List<Any?> =
    this.map { it.toPrimitive() }