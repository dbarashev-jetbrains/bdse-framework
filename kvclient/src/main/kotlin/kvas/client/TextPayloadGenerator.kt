package kvas.client

import kotlin.random.Random

fun generateRandomPhrase(): String {
    // Define a fixed vocabulary of 100-200 words
    val vocabulary = listOf(
        "the", "a", "an", "cat", "dog", "house", "tree", "bird", "swift", "lazy",
        "brown", "fox", "jumps", "over", "sleepy", "river", "stone", "walks", "quietly",
        "garden", "flower", "sun", "moon", "bright", "shines", "runs", "quickly", "behind",
        "large", "small", "tiny", "and", "by", "near", "beneath", "above", "sky", "cloud",
        "road", "winds", "long", "short", "path", "slowly", "towards", "away", "from", "into",
        "calls", "sits", "whispers", "softly", "happily", "loudly", "gentle", "blue", "green",
        "beautiful", "colour", "light", "dark", "forest", "mountain", "hill", "valley", "opens",
        "closes", "song", "melody", "echoes", "laugh", "child", "warm", "cold", "night", "day",
        "lives", "dreams", "starts", "ends", "corner", "room", "hidden", "shadow", "love", "hope"
    )

    // Randomly select 20-30 words from the vocabulary and form phrases
    val numWords = Random.nextInt(20, 31) // Randomly decide to generate between 20 to 30 words
    val phrases = mutableListOf<String>()

    // Generate pseudo-English phrases
    for (i in 1..numWords) {
        val word = vocabulary.random()
        phrases.add(word)
    }

    // Join words into sentences
    return phrases.joinToString(" ") { it }
}

