package kvas.node.storage

import kvas.client.KvasClient

class KvasStorageImpl(private val kvasClient: KvasClient): Storage {
    override fun put(rowKey: String, columnName: String, value: String) {
        kvasClient.put(rowKey, columnName, value)
    }

    override fun get(rowKey: String, columnName: String): String? {
        return kvasClient.doGet(rowKey, columnName)
    }

    override fun getRow(rowKey: String): StoredRow? {
        TODO("Not yet implemented")
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        TODO("Not yet implemented")
    }

}