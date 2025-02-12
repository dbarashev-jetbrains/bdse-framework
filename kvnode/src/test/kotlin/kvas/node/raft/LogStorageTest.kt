package kvas.node.raft

import kvas.proto.logEntryNumber
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LogStorageTest {
    @Test
    fun `new committed entry is rejected if less than the current`() {
        val logStorage = InMemoryLogStorage()
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 1
            termNumber = 1
        }
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 2
            termNumber = 1
        }
        assertEquals(2, logStorage.lastCommittedEntryNum.value.ordinalNumber)
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 1
            termNumber = 1
        }
        assertEquals(2, logStorage.lastCommittedEntryNum.value.ordinalNumber)
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 2
            termNumber = 0
        }
        assertEquals(2, logStorage.lastCommittedEntryNum.value.ordinalNumber)
    }

    @Test
    fun `subscriber is called on new committed entry change`() {
        val logStorage = InMemoryLogStorage()
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 1
            termNumber = 1
        }
        logStorage.lastCommittedEntryNum.subscribe { oldValue, newValue ->
            assertEquals(2, newValue.ordinalNumber)
            assertEquals(1, newValue.termNumber)
            true
        }
        logStorage.lastCommittedEntryNum.value = logEntryNumber {
            ordinalNumber = 2
            termNumber = 1
        }
        assertEquals(2, logStorage.lastCommittedEntryNum.value.ordinalNumber)
        assertEquals(1, logStorage.lastCommittedEntryNum.value.termNumber)
    }

}