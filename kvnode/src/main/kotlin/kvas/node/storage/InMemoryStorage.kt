package kvas.node.storage

import kvas.proto.KvasSharedProto.DataRow
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

class InMemoryStorage : Storage {
    private val key2value = ConcurrentSkipListMap<String, String>()
    private val lock = ReentrantReadWriteLock()
    private val readLock = lock.readLock()
    private val writeLock = lock.writeLock()
    private var logicalTimestamp = AtomicLong(0)
    private val rowKey2timestamp = mutableMapOf<String, Long>()

    override fun put(rowKey: String, columnName: String, value: String) {
        if (!writeLock.tryLock(1, TimeUnit.SECONDS)) {
            throw StorageException("Timeout while waiting for write lock")
        }
        try {
            val recordKey = if (columnName.isEmpty()) rowKey else "$rowKey:$columnName"
            key2value[recordKey] = value
            rowKey2timestamp[rowKey] = logicalTimestamp.incrementAndGet()
        } finally {
            writeLock.unlock()
        }
    }

    override fun get(rowKey: String, columnName: String): String? {
        if (!readLock.tryLock(1, TimeUnit.SECONDS)) {
            throw StorageException("Timeout while waiting for read lock")
        }
        return try {
            val recordKey = if (columnName.isEmpty()) rowKey else "$rowKey:$columnName"
            key2value[recordKey]
        } finally {
            readLock.unlock()
        }
    }

    override fun getRow(rowKey: String): StoredRow? {
        val scan = InMemoryRowScan(readLock, key2value.tailMap(rowKey), rowKey2timestamp) {
            it != rowKey
        }
        return scan.use {
            if (scan.hasNext()) {
                scan.next()
            } else {
                null
            }
        }
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        return conditions[MetadataKeys.ROW_KEY_EQ.name]?.let { rowKey ->
            InMemoryRowScan(readLock, key2value.tailMap(rowKey), rowKey2timestamp) {
                it != rowKey
            }
        } ?: InMemoryRowScan(readLock, key2value, rowKey2timestamp)
    }

    override val supportedFeatures: Map<String, String>
        get() = mapOf(
            MetadataKeys.ROW_KEY_EQ.name to "Scan will return only one row with the given key",
            MetadataKeys.LOGICAL_TIMESTAMP.name to "A single per-storage timestamp is incremented on each write"
        )

    override fun toString() = "In-memory storage"
}

class InMemoryRowScan(
    private val readLock: Lock,
    private val dataMap: MutableMap<String, String>,
    private val timestampMap: Map<String, Long>,
    private val stopCondition: (String) -> Boolean = { false }
) : RowScan {
    private val iterator: MutableIterator<Map.Entry<String, String>> = dataMap.iterator()
    private var lastReturnedRow: StoredRow? = null
    private var currentRow: StoredRow? = null
    private var currentDataEntry: Map.Entry<String, String>? = null
    override fun hasNext(): Boolean = currentRow != null

    init {
        if (!readLock.tryLock()) {
            throw StorageException("Timeout while waiting for read lock")
        }
        if (iterator.hasNext()) {
            currentRow = advance()
        }
    }

    override fun next(): StoredRow {
        lastReturnedRow = currentRow ?: throw NoSuchElementException()
        if (iterator.hasNext() || currentDataEntry != null) {
            currentRow = advance().let {
                if (stopCondition(it.key)) null else it
            }
        } else {
            currentRow = null
        }
        return lastReturnedRow!!
    }

    override fun remove() {
        lastReturnedRow?.let {
            it.valuesMap.forEach { (columnName, value) ->
                dataMap.remove(if (columnName == DEFAULT_COLUMN_NAME) it.key else "${it.key}:$columnName")
            }
        }
    }

    override fun close() {
        readLock.unlock()
    }


    private fun advance(): StoredRow {
        var currentRowKey = ""
        val values = mutableMapOf<String, String>()
        if (currentDataEntry == null) {
            currentDataEntry = iterator.next()
        }
        while (true) {
            var entry = currentDataEntry!!
            val entryKey = entry.key.substringBefore(':')
            if (currentRowKey == "" || entryKey == currentRowKey) {
                val columnName = entry.key.substringAfter(':', DEFAULT_COLUMN_NAME)
                currentRowKey = entryKey
                values[columnName] = entry.value
                if (iterator.hasNext()) {
                    currentDataEntry = iterator.next()
                } else {
                    currentDataEntry = null
                    break
                }
            } else {
                break
            }
        }
        return DataRow.newBuilder().setKey(currentRowKey).putAllValues(values)
            .setVersion(timestampMap[currentRowKey] ?: -1).build()
    }
}