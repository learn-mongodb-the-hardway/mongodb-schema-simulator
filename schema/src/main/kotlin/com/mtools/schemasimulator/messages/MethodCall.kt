package com.mtools.schemasimulator.messages

import java.util.concurrent.atomic.AtomicLong

class IdGenerator() {
    private var id: AtomicLong = AtomicLong(0)

    fun next() : Long {
        id.compareAndSet(Long.MAX_VALUE, 0)
        return id.incrementAndGet()
    }
}

val globalIdGenerator = IdGenerator()

abstract class MethodCall(val method: String, val id: Long = globalIdGenerator.next(), val type:String = "call")

abstract class MethodResponse(val method: String, val id: Long, val type:String = "response")
