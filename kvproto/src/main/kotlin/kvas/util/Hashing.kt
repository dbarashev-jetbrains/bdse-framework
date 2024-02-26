package kvas.util

import com.google.common.hash.Hashing
import java.nio.charset.Charset
import kotlin.math.log2
import kotlin.math.roundToInt


fun hashCode(key: String) = Hashing.murmur3_32().newHasher().putString(key, Charset.defaultCharset()).hash().asInt().toUInt()

object LinearHashing {
  fun Int.clearHighestBit() = this.toUInt().and((this.takeHighestOneBit() - 1).toUInt()).toInt()

  fun bitmask(num: Int): UInt =
    ((1 shl log2(num.toDouble()).roundToInt()) - 1).toUInt()

  fun shardNumber(key: String, shardCount: Int): Int {
    val lastBits = hashCode(key).and(bitmask(shardCount)).toInt()
    return if (lastBits < shardCount) {
      lastBits
    } else {
      lastBits.clearHighestBit()
    }
  }

  fun splitShardNumber(shardNumber: Int) = shardNumber.clearHighestBit()
}

