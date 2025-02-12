// PLEASE DON'T EDIT THIS FILE
package kvas.node.storage

import kvas.proto.KvasSharedProto
import kvas.proto.KvasSharedProto.DataRow

const val DEFAULT_COLUMN_NAME = ""

class StorageException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Provides a storage interface for handling key-value data with optional column support.
 */
interface Storage {
    /**
     * Executes `put` operation and inserts or updates a value of a column with the given `columnName` in a row
     * identified with the `rowKey`.
     */
    @Throws(StorageException::class)
    fun put(rowKey: String, columnName: String = DEFAULT_COLUMN_NAME, value: String)

    /**
     * Executes `get` operation and returns a value of a column with the given `columnName` in a row
     * identified with the `rowKey`. Returns `null` if such value does not exist.
     */
    @Throws(StorageException::class)
    fun get(rowKey: String, columnName: String = DEFAULT_COLUMN_NAME): String?

    /**
     * Executes `get` operation and returns the whole row identified with the `rowKey`.
     * All columns where the value is defined in this row are expected to be in the result.
     * @return a StoredRow instance with all defined column values or null if such row does not exist.
     */
    @Throws(StorageException::class)
    fun getRow(rowKey: String): StoredRow?

    /**
     * Starts a scan with the given scan conditions. The conditions are implementation-specific. For instance,
     * an implementation may provide an option to supply `MetadataKeys.ROW_KEY_EQ.name` value to scan for a single
     * row with the given keys.
     *
     * The implementation must declare the supported conditions in the docs and in `supportedScanConditions` method
     * below, so that they could be printed when starting a node.
     */
    @Throws(StorageException::class)
    fun scan(conditions: Map<String, String> = emptyMap()): RowScan

    /**
     * Returns the supported storage features, as a map of the feature key to some human-readable comment.
     */
    val supportedFeatures: Map<String, String>
        get() = emptyMap()
}

/**
 * Enumerates "standard" scan conditions/metadata keys that may be implemented by the storage implementations.
 */
enum class MetadataKeys(val comment: String) {
    ROW_KEY_EQ("If used in the scan conditions, returns a single row with the given row key."),

    LOGICAL_TIMESTAMP(
        """
    *
    * If used in the scan condition and supported by the storage, returns data at the specified logical timestamp.
    * If used in the `StoredRow` metadata, indicates the logical timestamp of the row.
    */
  """.trimMargin("*")
    )
}

/**
 * A stored row instance may hold values of one or more columns, depending on the requirements This data class holds the values and metadata of a single stored row. The values property maps column names to
 * values. If there is just a single "default" column, it contains a single entry with DEFAULT_COLUMN_NAME as a key.
 *
 * The rowMetadata property may contain implementation-specific metadata, such as the logical timestamp of the latest
 * read or write, or other metadata that the storage implementation may decide to expose.
 */
typealias StoredRow = KvasSharedProto.DataRow

/**
 * Creates a new instance of a stored `DataRow` by setting its key and values.
 *
 * @param key The key to be associated with the DataRow.
 * @param values A map of values to be stored in the DataRow.
 * @return A newly built DataRow object containing the specified key and values.
 */
fun createStoredRow(key: String, values: Map<String, String>) =
    DataRow.newBuilder().setKey(key).putAllValues(values).build()

/**
 * An object that allows for scanning and removal of the stored rows.
 */
interface RowScan : MutableIterator<StoredRow>, AutoCloseable