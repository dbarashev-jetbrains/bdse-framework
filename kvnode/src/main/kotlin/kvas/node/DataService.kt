/**
 *
 */
package kvas.node

import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.proto.KvasProto.*
import kvas.setup.Sharding
import kvas.util.NodeAddress
import org.slf4j.LoggerFactory
import kotlin.concurrent.timer


/**
 * Represents a KVAS data node.
 * The node is responsible for storing the shard data.
 *
 * @param selfAddress The address of the node, containing host and port information.
 * @param storage The storage interface for retrieving and persisting key-value data.
 * @param sharding The sharding mechanism used for shard lookup and token generation.
 * @param initShardToken A supplier function to retrieve a stored shard token. The default function returns null,
 *                       that means there is no stored shard token for this node.
 * @param registerNode A function to handle node registration requests with the metadata server.
 */
open class KvasDataNode(
    private val selfAddress: NodeAddress,
    private val storage: Storage,
    private val sharding: Sharding,
    initShardToken: () -> Int? = { null },
    private val registerNode: (RegisterNodeRequest) -> RegisterNodeResponse,
    private val dataTransferService: DataTransferServiceGrpcKt.DataTransferServiceCoroutineImplBase,
) : DataServiceGrpcKt.DataServiceCoroutineImplBase() {

    private var shardToken: Int? = initShardToken()
    private var clusterMetadata = clusterMetadata { }


    init {
        // We will send registration requests to the metadata server once per 10 seconds.
        timer(name = "Register node", period = 10000, initialDelay = 1000) {
            registerItself()
        }
    }

    /**
     * Retrieves the value associated with a specific key and column from the storage.
     * Validation is performed to ensure that the request is targeting the correct shard.
     * If the validation fails, a response indicating a need to refresh shards is returned.
     *
     * @param request The request object containing the key (`rowKey`), column name (`columnName`),
     *                and a shard token that is known to the client.
     * @return A `GetValueResponse` object containing the retrieved value if successful,
     *         or a status code indicating the need to refresh shard information.
     */
    override suspend fun getValue(request: KvasProto.GetValueRequest): KvasProto.GetValueResponse {
        return if (validateShard(request.rowKey, request.shardToken)) {
            try {
                val value = storage.get(request.rowKey, request.columnName)
                getValueResponse {
                    if (value != null) {
                        this.value = StringValue.of(value)
                    }
                    this.code = KvasProto.GetValueResponse.StatusCode.OK
                }
            } catch (ex: Throwable) {
                LOG.error("Failed to GET key={}", request.rowKey, ex)
                getValueResponse {
                    this.code = KvasProto.GetValueResponse.StatusCode.STORAGE_ERROR
                }
            }
        } else {
            LOG.debug("Invalid shard, the client needs to refresh its metadata")
            getValueResponse {
                this.code = KvasProto.GetValueResponse.StatusCode.REFRESH_SHARDS
            }
        }
    }

    /**
     * Handles a request to put a value into the storage. The operation involves validating
     * the shard information associated with the provided key. If the validation is successful,
     * the value is stored in the specified column for the given key. Otherwise, a response
     * indicating the need to refresh shard information is returned.
     *
     * @param request The request object containing the key (`rowKey`), column name (`columnName`),
     *                value (`value`), and a shard token known to the client.
     * @return A `PutValueResponse` object with a status code indicating the result of the operation.
     *         If successful, the status code will be `OK`. If validation fails, the status code
     *         will be `REFRESH_SHARDS`.
     */
    override suspend fun putValue(request: KvasProto.PutValueRequest): KvasProto.PutValueResponse {
        return if (validateShard(request.rowKey, request.shardToken)) {
            try {
                storage.put(request.rowKey, request.columnName, request.value)
                LOG.debug(
                    "PUT success for key={}, column={}, value={}",
                    request.rowKey,
                    request.columnName,
                    request.value
                )
                putValueResponse {
                    this.code = KvasProto.PutValueResponse.StatusCode.OK
                }
            } catch (ex: Throwable) {
                LOG.error("Failed to PUT key={}", request.rowKey, ex)
                putValueResponse {
                    this.code = KvasProto.PutValueResponse.StatusCode.STORAGE_ERROR
                }
            }
        } else {
            LOG.debug("Invalid shard, the client needs to refresh its metadata")
            putValueResponse {
                this.code = KvasProto.PutValueResponse.StatusCode.REFRESH_SHARDS
            }
        }
    }

    protected open fun validateShard(rowKey: String, requestToken: Int): Boolean {
        val expectedShard = this.sharding.calculateShard(rowKey, this.clusterMetadata)
        LOG.debug(
            "Validate: key={} token={} expectedShard={} myToken={}",
            rowKey,
            requestToken,
            expectedShard,
            this.shardToken
        )
        if (requestToken != expectedShard.shardToken) {
            LOG.debug(
                "Invalid shard for key={} (shard={}), expectedShard={}, myToken={}",
                rowKey,
                requestToken,
                expectedShard,
                this.shardToken
            )
            return false
        }
        return this.shardToken?.let { it == requestToken } ?: false
    }

    internal fun registerItself() {
        val log = LoggerFactory.getLogger("Node.RegisterItself")
        // Register at the metadata server, supplying the assigned token if it is available.
        val response = registerNode(registerNodeRequest {
            this.nodeAddress = selfAddress.toString()
            this@KvasDataNode.shardToken?.let {
                this.shardToken = Int32Value.of(it)
            }
        })
        when (response.code) {
            KvasProto.RegisterNodeResponse.StatusCode.OK -> {
                log.debug("Registered with token={}", response.shardToken)
                this.shardToken = response.shardToken
                this.clusterMetadata = response.metadata
            }

            else -> {
                log.error("Can't register at the metadata master, received {}", response)
                System.exit(1)
            }
        }
    }

    /**
     * Handles a sharding change notification from the metadata server, updating the cached metadata
     * with the provided sharding information.
     *
     * @param request The `ShardingChangeRequest` object containing the updated
     *                cluster metadata and sharding information.
     */
    internal open fun onShardingChange(metadata: ClusterMetadata) {
        LOG.debug("Received a sharding change notification. The new shards={}", metadata)
        this.clusterMetadata = metadata
        // TODO: write your code here and implement data transfer on re-sharding
    }

    internal open fun createDataTransferService(): DataTransferServiceGrpcKt.DataTransferServiceCoroutineImplBase {
        return dataTransferService
    }
}

internal class MetadataListenerImpl(private val onChange: (ShardingChangeRequest) -> Unit) :
    MetadataListenerGrpcKt.MetadataListenerCoroutineImplBase() {
    override suspend fun shardingChange(request: ShardingChangeRequest): KvasProto.ShardingChangeResponse {
        onChange(request)
        return shardingChangeResponse { }
    }
}

private val LOG = LoggerFactory.getLogger("DataService")