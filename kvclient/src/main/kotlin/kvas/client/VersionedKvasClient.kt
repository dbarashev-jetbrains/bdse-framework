package kvas.client

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class VersioningRecord(var versions: MutableList<Int> = mutableListOf())

/**
 * This is a wrapper around [KvasClient] that emulates versioning support.
 * For every row key K and column name C, it stores the list of all available versions of that cell in K@.C column
 * (the row key is K@ and the column name is C).
 * Numbers of all available versions are stored in K@.C column. Value associated with some particular version V
 * is stored in K@V.C column.
 *
 * The client supports two operations:
 * - get(rowKey, columnName, maxVersion) - returns the value of the latest version of the cell with the given row key
 *   and column name that is less than or equal to the specified maxVersion.
 * - put(rowKey, columnName, value, version) - stores the given value in the cell with the given row key and column name
 *   at the specified version.
 */
class VersionedKvasClient(val kvasClient: KvasClient) {
    /**
     * Returns the value of the requested cell with the highest version less than or equal to the specified maxVersion.
     * If the maxVersion is null, the latest version is returned.
     */
    fun get(rowKey: String, columnName: String, maxVersion: Int? = null): String? {
        val versionsValue = kvasClient.doGet("$rowKey@", columnName) ?: return null
        val record = Json.decodeFromString<VersioningRecord>(versionsValue)
        val upperBoundVersion = if (maxVersion != null) {
            record.versions.lastOrNull { it <= maxVersion }
        } else record.versions.lastOrNull()
        return upperBoundVersion?.let { kvasClient.doGet("$rowKey@$upperBoundVersion", columnName) }
    }

    /**
     * Stores the given value in the cell with the given row key and column name at the specified version.
     * If the version is null, the value is stored at the latest version + 1.
     * Updated list of all available versions is stored in K@.C column.
     */
    fun put(rowKey: String, columnName: String, value: String, version: Int? = null) {
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