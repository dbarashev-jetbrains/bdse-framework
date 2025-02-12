package kvas.util

import com.google.common.hash.Hashing
import java.nio.charset.Charset
import kotlin.math.log2
import kotlin.math.roundToInt


/**
 * Returns Murmur3_32 hash code of the given string. The hash code is stable, even across VM runs (it will always return
 * the same code for the same input)
 */
fun hashCode(key: String) = Hashing.murmur3_32_fixed().hashString(key, Charset.defaultCharset()).asInt().toUInt()

/**
 * This object encapsulates functions for linear hashing scheme.
 */
object LinearHashing {
    /**
     * Clears the highest significant bit of this integer.
     */
    fun Int.clearHighestBit() = this.toUInt().and((this.takeHighestOneBit() - 1).toUInt()).toInt()

    private fun bitmask(num: Int): UInt =
        ((1 shl log2(num.toDouble()).roundToInt()) - 1).toUInt()

    /**
     * Calculates a shard number from the hash code and total shard count using linear hashing method.
     */
    fun shardNumber(key: String, shardCount: Int): Int {
        val lastBits = hashCode(key).and(bitmask(shardCount)).toInt()
        return if (lastBits < shardCount) {
            lastBits
        } else {
            lastBits.clearHighestBit()
        }
    }

    /**
     * Returns a number of a shard that is going to split after adding shard number shardNumber.
     */
    fun splitShardNumber(shardNumber: Int) = shardNumber.clearHighestBit()
}

