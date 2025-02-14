package kvas.setup

import kvas.proto.KvasMetadataProto.*
import kvas.proto.replicatedShard

/**
 * The sharding strategy interface. It provides the following functions:
 * - find an appropriate shard to cll, given they search key and the list of all shards;
 * - given a list of existing shards, calculate a "token" for the new shard.
 *
 * The meaning of "token" varies depending on the chosen sharding algorithm. For instance, in the naive algorithm, where
 * a shard number of a given key is its hash code modulo the total number of shards, tokens are all possible remainders of
 * division.
 */
interface Sharding {
    /**
     * Determines the appropriate shard (node) that corresponds to the given key
     * within the context of the provided cluster metadata.
     *
     * @param key The key for which the corresponding shard is to be calculated.
     * @param metadata The cluster metadata containing information about the available shards.
     * @return The NodeInfo of the shard responsible for the given key.
     */
    fun calculateShard(key: String, metadata: ClusterMetadata): ReplicatedShard

    /**
     * Creates a new token for a shard based on the provided list of existing shard information.
     *
     * @param shards The list of existing shard metadata, where each node is represented by a NodeInfo object.
     * @return An integer representing the token for the newly created shard.
     */
    fun createNewToken(shards: List<NodeInfo>): Int
}

/**
 * A naive implementation of the `Sharding` interface, which assigns shards
 * using a simplistic modulus-based algorithm.
 *
 * The shard is determined by calculating the hash code of the given key
 * and taking its modulus with the total number of shards in the cluster.
 */
object NaiveSharding : Sharding {
    override fun calculateShard(key: String, metadata: ClusterMetadata): ReplicatedShard {
        val hashCode = kvas.util.hashCode(key)
        val shardNumber = (hashCode % metadata.shardsCount.toUInt()).toInt()
        return metadata.shardsList[shardNumber]
    }

    override fun createNewToken(shards: List<NodeInfo>): Int {
        return shards.size
    }

    override fun toString(): String {
        return "NaiveSharding (key hash code modulo shard count)"
    }
}

/**
 * An object representing a placeholder implementation of the `Sharding` interface,
 * intended as a temporary or unimplemented stand-in for a proper sharding implementation.
 *
 * This object provides default implementations of the `Sharding` interface methods,
 * which currently throw `NotImplementedError` when invoked.
 */
object NotImplementedSharding : Sharding {
    override fun calculateShard(key: String, metadata: ClusterMetadata): ReplicatedShard {
        TODO("Not yet implemented")
    }

    override fun createNewToken(shards: List<NodeInfo>): Int {
        TODO("Not yet implemented")
    }

    override fun toString(): String {
        return "This sharding is not yet implemented"
    }
}

/**
 * Raft sharding is not really a sharding. It is rather a hack to let the client find out the Raft nodes from the
 * cluster metadata.
 */
object RaftSharding : Sharding {
    override fun calculateShard(key: String, metadata: ClusterMetadata): ReplicatedShard {
        return replicatedShard {
            this.leader = metadata.raftGroup.nodesList.first()
            this.followers.addAll(metadata.raftGroup.nodesList.drop(1))
        }
    }

    override fun createNewToken(shards: List<NodeInfo>): Int {
        TODO("Not supposed to be implemented")
    }

    override fun toString(): String {
        return "Raft sharding serves for the client purposes only."
    }


}

/**
 * Object that maintains a collection of predefined sharding methods used for distributed systems.
 * This object serves as a centralized registry for different available sharding techniques.
 */
object AllShardings {
    val NAIVE = "naive" to NaiveSharding

    // TODO("Task 2"): replace with your own implementation
    val LINEAR = "linear" to NotImplementedSharding

    // TODO("Task 2"): replace with your own implementation
    val CONSISTENT_HASHING = "consistent-hashing" to NotImplementedSharding

    val RAFT = "raft" to RaftSharding
    val ALL = listOf(NAIVE, LINEAR, CONSISTENT_HASHING, RAFT).toMap()
}