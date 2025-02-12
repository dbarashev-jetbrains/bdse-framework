package kvas.util

import kotlin.properties.Delegates


typealias ObservableSubscriber<T> = (T, T) -> Boolean

class ObservableProperty<T>(initialValue: T) {
    private val subscribers = mutableListOf<ObservableSubscriber<T>>()

    var value by Delegates.vetoable(initialValue) { _, old, new ->
        if (old != new) {
            subscribers.all { it(old, new) }
        } else {
            true
        }
    }

    fun subscribe(subscriber: ObservableSubscriber<T>) {
        subscribers.add(subscriber)
    }

    override fun toString(): String {
        return "$value"
    }
}