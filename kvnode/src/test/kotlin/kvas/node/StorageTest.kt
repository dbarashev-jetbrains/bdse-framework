package kvas.node

import kvas.node.storage.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class StorageTest {
    fun createStorage() = InMemoryStorage()

    @Test
    fun `row scan test`() {
        val storage = createStorage()
        storage.put("1", columnName = "id", value = "1")
        storage.put("1", columnName = "name", value = "Foo")
        storage.put("2", columnName = "id", value = "2")
        storage.put("2", columnName = "name", value = "Bar")
        storage.put("3", value = "3")

        assertEquals(
            listOf(
                createStoredRow("1", mapOf("id" to "1", "name" to "Foo")).toBuilder().setVersion(2).build(),
                createStoredRow("2", mapOf("id" to "2", "name" to "Bar")).toBuilder().setVersion(4).build(),
                createStoredRow("3", mapOf(DEFAULT_COLUMN_NAME to "3")).toBuilder().setVersion(5).build()
            ), storage.scan().asSequence().toList()
        )
    }

    @Test
    fun `row remove test`() {
        val storage = createStorage()
        storage.put("1", columnName = "id", value = "1")
        storage.put("1", columnName = "name", value = "Foo")
        storage.put("2", columnName = "id", value = "2")
        storage.put("2", columnName = "name", value = "Bar")
        storage.put("3", value = "3")

        // We scan through the entire row set and request removal of the second one. The removed row will remain in the
        // current scan results, however, we expect that we will not see it in the next scan.
        val rows = mutableListOf<StoredRow>()
        storage.scan().let {
            rows.add(it.next())
            rows.add(it.next())
            it.remove()
            rows.add(it.next())
            it.close()
        }
        assertEquals(
            listOf(
                createStoredRow("1", mapOf("id" to "1", "name" to "Foo")).toBuilder().setVersion(2).build(),
                createStoredRow("2", mapOf("id" to "2", "name" to "Bar")).toBuilder().setVersion(4).build(),
                createStoredRow("3", mapOf(DEFAULT_COLUMN_NAME to "3")).toBuilder().setVersion(5).build()
            ), rows
        )
        assertEquals(
            listOf(
                createStoredRow("1", mapOf("id" to "1", "name" to "Foo")).toBuilder().setVersion(2).build(),
                createStoredRow("3", mapOf(DEFAULT_COLUMN_NAME to "3")).toBuilder().setVersion(5).build()
            ), storage.scan().asSequence().toList()
        )
    }

    @Test
    fun `scan locks the storage for the duration of the scan`() {
        val storage = createStorage()
        storage.put("1", value = "1")
        storage.scan().let { scan ->
            scan.next()

            assertThrows(StorageException::class.java) {
                storage.put("2", value = "2")
            }

            scan.close()
            storage.put("2", value = "2")
            assertEquals("2", storage.get("2"))
        }
    }

    @Test
    fun `scan condition - row key equals`() {
        val storage = createStorage()
        storage.put("1", columnName = "id", value = "1")
        storage.put("1", columnName = "name", value = "Foo")
        storage.put("2", columnName = "id", value = "2")
        storage.put("2", columnName = "name", value = "Bar")
        storage.put("3", value = "3")

        assertEquals(
            listOf(
                createStoredRow("1", mapOf("id" to "1", "name" to "Foo")).toBuilder().setVersion(2).build()
            ), storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "1")).asSequence().toList()
        )

        assertEquals(
            listOf(
                createStoredRow("2", mapOf("id" to "2", "name" to "Bar")).toBuilder().setVersion(4).build()
            ), storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "2")).asSequence().toList()
        )

        assertEquals(
            listOf(
                createStoredRow("3", mapOf(DEFAULT_COLUMN_NAME to "3")).toBuilder().setVersion(5).build()
            ), storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "3")).asSequence().toList()
        )

        assertEquals(
            emptyList<StoredRow>(),
            storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "4")).asSequence().toList()
        )
    }

    @Test
    fun `row version increments`() {
        val storage = createStorage()
        storage.put("1", columnName = "id", value = "1")
        val version1 = storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "1")).use { scan ->
            scan.next().version
        }

        storage.put("1", columnName = "name", value = "Foo")
        val version2 = storage.scan(mapOf(MetadataKeys.ROW_KEY_EQ.name to "1")).use { scan -> scan.next().version }

        assertTrue(version1 < version2) {
            "Expected version $version1 to be less than $version2"
        }
    }
}