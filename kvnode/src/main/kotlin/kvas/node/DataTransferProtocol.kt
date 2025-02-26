package kvas.node

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.DataTransferServiceGrpc.DataTransferServiceBlockingStub
import kvas.proto.KvasSharedProto.DataRow
import kvas.setup.Sharding
import kvas.util.GrpcPool
import kvas.util.NodeAddress
import kvas.util.toNodeAddress

/**
 * Defines the methods required for a data transfer source.
 *
 * Please read the comments in data_transfer.proto for the details of the data transfer protocol
 */
interface DataTransferSource {
    fun initiateDataTransfer(request: KvasDataTransferProto.InitiateDataTransferRequest): KvasDataTransferProto.InitiateDataTransferResponse
    fun startDataTransfer(request: KvasDataTransferProto.StartDataTransferRequest): Flow<DataRow>
    fun finishDataTransfer(request: KvasDataTransferProto.FinishDataTransferRequest): KvasDataTransferProto.FinishDataTransferResponse
}

/**
 * Defines the methods required from a data transfer destination.
 *
 * Please read the comments in data_transfer.proto for the details of the data transfer protocol
 */
interface DataTransferDestination {
    /**
     * This method is called on every change in the list of shards (e.g. when a new node registers, including the one where
     * this method is invoked). The implementation is supposed to initiate data transfer protocol by calling the
     * methods of DataTransferService on other cluster nodes.
     */
    fun onMetadataChange(
        metadata: KvasMetadataProto.ClusterMetadata,
        grpcPool: GrpcPool<DataTransferServiceBlockingStub>
    )
}

/**
 * This interface combines the methods of a data transfer source and destination, just for convenience.
 */
interface DataTransferProtocol : DataTransferSource, DataTransferDestination

/**
 * All available implementations of the data transfer protocol.
 */
object DataTransferProtocols {
    val DEMO = "demo" to ::DemoDataTransferProtocol
    val LINEAR = "linear" to { sharding: Sharding, selfAddress: NodeAddress, storage: Storage ->
        TODO("Task2: implement a Data Transfer Protocol to allow for re-sharding")
    }
    val CONSISTENT_HASHING = "consistent-hashing" to { sharding: Sharding, selfAddress: NodeAddress, storage: Storage ->
        TODO("Task2: implement a Data Transfer Protocol to allow for re-sharding")
    }
    val ALL = listOf(DEMO, LINEAR, CONSISTENT_HASHING).toMap()
}


/**
 * This is a demo implementation of a data transfer protocol that just emits all values from the storage to any node that
 * requests them, and just puts all the values to the storage on the destination node. De-facto it creates a replica of
 * data from all nodes in the cluster.
 */
class DemoDataTransferProtocol(
    private val sharding: Sharding,
    private val selfAddress: NodeAddress,
    private val storage: Storage
) : DataTransferProtocol {
    override fun initiateDataTransfer(request: KvasDataTransferProto.InitiateDataTransferRequest) =
        initiateDataTransferResponse {
            this.transferId = -1
        }

    override fun startDataTransfer(request: KvasDataTransferProto.StartDataTransferRequest): Flow<DataRow> {
        try {
            return storage.scan().use {
                it.asFlow().map { DataRow.newBuilder().setKey(it.key).putAllValues(it.valuesMap).build() }
            }
        } catch (ex: Throwable) {
            LOG.error("Failed to scan storage", ex)
            return emptyList<DataRow>().asFlow()
        }
    }

    override fun finishDataTransfer(request: KvasDataTransferProto.FinishDataTransferRequest) =
        finishDataTransferResponse {
        }

    override fun onMetadataChange(
        metadata: KvasMetadataProto.ClusterMetadata,
        grpcPool: GrpcPool<DataTransferServiceBlockingStub>
    ) {
        metadata.shardsList.forEach {
            if (it.leader.nodeAddress != selfAddress.toString()) {
                transferData(it, grpcPool)
            }
        }
    }

    private fun transferData(
        shard: KvasMetadataProto.ReplicatedShard,
        grpcPool: GrpcPool<DataTransferServiceBlockingStub>
    ) {
        val nodeAddess = shard.leader.nodeAddress.toNodeAddress()
        grpcPool.rpc(nodeAddess) {
            LOG.info("START Transferring data for shard={} from node={}", shard, nodeAddess)
            val initResponse = initiateDataTransfer(initiateDataTransferRequest {
                this.requesterAddress = selfAddress.toString()
            })
            if (initResponse.transferId == -1) {
                startDataTransfer(startDataTransferRequest {
                    this.transferId = -1
                }).forEach { dataRow ->
                    println("transferred data row=$dataRow")
                    dataRow.valuesMap.forEach { (k, v) -> storage.put(dataRow.key, k, v) }
                }
                finishDataTransfer(finishDataTransferRequest {
                    this.transferId = -1
                })
            }
            LOG.info("FINISH Transferring data for shard={} from node={}", shard, nodeAddess)
        }
    }
}

/**
 * This is a void implementation of the data transfer protocol. It does nothing.
 */
class VoidDataTransferProtocol : DataTransferProtocol {
    override fun initiateDataTransfer(request: KvasDataTransferProto.InitiateDataTransferRequest) =
        initiateDataTransferResponse {
            this.transferId = -1
        }

    override fun startDataTransfer(request: KvasDataTransferProto.StartDataTransferRequest): Flow<DataRow> {
        return emptyList<DataRow>().asFlow()
    }

    override fun finishDataTransfer(request: KvasDataTransferProto.FinishDataTransferRequest): KvasDataTransferProto.FinishDataTransferResponse {
        return finishDataTransferResponse {}
    }

    override fun onMetadataChange(
        metadata: KvasMetadataProto.ClusterMetadata,
        grpcPool: GrpcPool<DataTransferServiceBlockingStub>
    ) {

    }
}

private val LOG = org.slf4j.LoggerFactory.getLogger("Node.MoveData")