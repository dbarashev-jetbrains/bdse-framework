package kvas.node

import kvas.node.storage.RowScan
import kvas.node.storage.Storage
import kvas.node.storage.StoredRow
import kvas.proto.KvasStatsProto
import kvas.proto.StatisticsGrpcKt
import kvas.proto.getStatisticsResponse
import kvas.proto.resetStatisticsResponse
import java.util.concurrent.atomic.AtomicInteger

class StatisticsStorage(private val delegateStorage: Storage) : Storage {
    internal var putTotal = AtomicInteger(0)
    internal var putSuccess = AtomicInteger(0)
    internal var getTotal = AtomicInteger(0)
    internal var getSuccess = AtomicInteger(0)

    internal val readSuccessRate: Double get() = getSuccess.toDouble() / getTotal.toDouble()
    internal val writeSuccessRate: Double get() = putSuccess.toDouble() / putTotal.toDouble()

    override fun put(rowKey: String, columnName: String, value: String) {
        putTotal.incrementAndGet()
        delegateStorage.put(rowKey, columnName, value)
        putSuccess.incrementAndGet()
    }

    override fun get(rowKey: String, columnName: String): String? {
        getTotal.incrementAndGet()
        return delegateStorage.get(rowKey, columnName)?.also { getSuccess.incrementAndGet() }
    }

    override fun getRow(rowKey: String): StoredRow? {
        getTotal.incrementAndGet()
        return delegateStorage.getRow(rowKey)?.also { getSuccess.incrementAndGet() }
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        return delegateStorage.scan(conditions)
    }

    override val supportedFeatures: Map<String, String> get() = delegateStorage.supportedFeatures
    fun reset() {
        getTotal.set(0)
        getSuccess.set(0)
        putTotal.set(0)
        putSuccess.set(0)
    }
}

class StatisticsService(private val storage: StatisticsStorage) : StatisticsGrpcKt.StatisticsCoroutineImplBase() {
    override suspend fun getStatistics(request: KvasStatsProto.GetStatisticsRequest): KvasStatsProto.GetStatisticsResponse {
        return getStatisticsResponse {
            readSuccessRate = storage.readSuccessRate
            writeSuccessRate = storage.writeSuccessRate
            readTotal = storage.getTotal.get()
            writeTotal = storage.putTotal.get()
        }
    }

    override suspend fun resetStatistics(request: KvasStatsProto.ResetStatisticsRequest): KvasStatsProto.ResetStatisticsResponse {
        storage.reset()
        return resetStatisticsResponse { }
    }
}