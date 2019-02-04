package com.mtools.schemasimulator.logger

import com.mtools.schemasimulator.messages.worker.MetricsResult
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

interface MetricLogger {
    fun createLogEntry(simulation: String, tick: Long): LogEntry
    fun toMetricResult(): MetricsResult
}

class LogEntry(val name: String, val tick: Long, val entries : MutableList<Pair<String, Long>> = mutableListOf()) {
    var total: Long = 0

    fun add(tag: String, time: Long) {
        entries += tag to time
    }
}

class NoopLogger: MetricLogger {
    override fun toMetricResult(): MetricsResult = MetricsResult(mapOf())

    override fun createLogEntry(simulation: String, tick: Long): LogEntry = LogEntry(simulation, tick)
}

class InMemoryMetricLogger(val name: String) : MetricLogger {
    private val logEntries = ConcurrentHashMap<Long, MutableList<LogEntry>>()

    override fun toMetricResult(): MetricsResult {
        return MetricsResult(logEntries)
    }

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        if (!logEntries.containsKey(tick)) {
            logEntries[tick] = CopyOnWriteArrayList<LogEntry>()
        }

        val logEntry = LogEntry(simulation, tick)
        logEntries[tick]!! += logEntry
        return logEntry
    }
}
