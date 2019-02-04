package com.mtools.schemasimulator.logger

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.messages.worker.MetricsResult
import org.java_websocket.WebSocket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

interface MetricLogger {
    fun createLogEntry(simulation: String, tick: Long): LogEntry
}

class LogEntry(val name: String, val tick: Long, val entries : MutableList<Pair<String, Long>> = mutableListOf()) {
    var total: Long = 0

    fun add(tag: String, time: Long) {
        entries += tag to time
    }
}

class NoopLogger: MetricLogger {
    override fun createLogEntry(simulation: String, tick: Long): LogEntry = LogEntry(simulation, tick)
}

class InMemoryMetricLogger(val name: String, val conn: WebSocket, val cutOff: Int = 500) : MetricLogger {
    private var logEntries = CopyOnWriteArrayList<LogEntry>()

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        synchronized(this) {
            val logEntry = LogEntry(simulation, tick)
            logEntries.add(logEntry)

            if (logEntries.size == cutOff) {
                // Get a reference
                val list = logEntries
                // Empty the list
                logEntries = CopyOnWriteArrayList()
                // Send a metrics message
                conn.send(Klaxon().toJsonString(MetricsResult(list)))
            }

            return logEntry
        }
    }
}
