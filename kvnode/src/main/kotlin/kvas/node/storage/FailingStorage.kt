package kvas.node.storage

import com.github.michaelbull.result.getOrThrow

class FailingStorage(val failureEmulator: OutageEmulator, val delegate: Storage) : Storage {
    override fun put(rowKey: String, columnName: String, value: String) {
        failureEmulator.serveIfAvailable { delegate.put(rowKey, columnName, value) }.getOrThrow { StorageException(it) }
    }

    override fun get(rowKey: String, columnName: String): String? {
        return failureEmulator.serveIfAvailable { delegate.get(rowKey, columnName) }.getOrThrow { StorageException(it) }
    }

    override fun getRow(rowKey: String): StoredRow? {
        return failureEmulator.serveIfAvailable { delegate.getRow(rowKey) }.getOrThrow { StorageException(it) }
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        return failureEmulator.serveIfAvailable { delegate.scan(conditions) }.getOrThrow { StorageException(it) }
    }

    override val supportedFeatures: Map<String, String>
        get() = delegate.supportedFeatures
}