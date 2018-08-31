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

abstract class MethodCall(val method: String) {
    val type:String = "call"
    val id: Long = globalIdGenerator.next()
}

abstract class MethodResponse(val method: String, val id: Long) {
    val type:String = "response"
    val ok: Boolean = true
}

abstract class MethodErrorResponse(val method: String, val id: Long, val message: String, val errorCode: Int) {
    val type:String = "error"
    val ok: Boolean = false
}

