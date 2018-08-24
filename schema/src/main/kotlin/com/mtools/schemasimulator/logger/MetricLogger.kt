package com.mtools.schemasimulator.logger

interface MetricLogger {
    fun createLogEntry(simulation: String): LogEntry
}

class LogEntry(val name: String, val entries : MutableList<Pair<String, Long>> = mutableListOf()) {
    var total: Long = 0

    fun add(tag: String, time: Long) {
        entries += tag to time
    }
}

class NoopLogger: MetricLogger {
    override fun createLogEntry(simulation: String): LogEntry = LogEntry(simulation)
}
