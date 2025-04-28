package kvas.client

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class VersioningRecord(var versions: MutableList<Int> = mutableListOf())

class VersionedKvasClient(private val kvasClient: KvasClient) {
    fun get(rowKey: String, columnName: String, maxVersion: Int?): String? {
        val versionsValue = kvasClient.doGet("$rowKey@", columnName) ?: return null
        val record = Json.decodeFromString<VersioningRecord>(versionsValue)
        val upperBoundVersion = if (maxVersion != null) {
            record.versions.lastOrNull { it <= maxVersion }
        } else record.versions.lastOrNull()
        return upperBoundVersion?.let { kvasClient.doGet("$rowKey@$upperBoundVersion", columnName) }
    }

    fun put(rowKey: String, columnName: String, value: String, version: Int?) {
        val versionsRecord = kvasClient.doGet("$rowKey@", columnName)?.let { Json.decodeFromString<VersioningRecord>(it) }
            ?: VersioningRecord()
        val newVersion = version ?: versionsRecord.versions.lastOrNull()?.inc() ?: 1
        if (!versionsRecord.versions.contains(newVersion)) {
            versionsRecord.versions.add(newVersion)
            versionsRecord.versions.sort()
        }
        kvasClient.put("$rowKey@$newVersion", columnName, value)
        kvasClient.put("$rowKey@", columnName, Json.encodeToString(versionsRecord))
    }
}