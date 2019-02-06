package com.mtools.schemasimulator.logger

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.messages.worker.MetricsResult
import java.net.URI
import java.util.concurrent.CopyOnWriteArrayList

interface MetricLogger {
    fun createLogEntry(simulation: String, tick: Long): LogEntry
    fun flush()
}

class LogEntry(val name: String, val tick: Long, val entries : MutableList<Pair<String, Long>> = mutableListOf(), var total: Long = 0) {
    fun add(tag: String, time: Long) {
        entries += tag to time
    }
}

class NoopLogger: MetricLogger {
    override fun flush() {}

    override fun createLogEntry(simulation: String, tick: Long): LogEntry = LogEntry(simulation, tick)
}

class LocalMetricLogger() : MetricLogger {
    var logEntries = CopyOnWriteArrayList<LogEntry>()

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        val logEntry = LogEntry(simulation, tick)
        logEntries.add(logEntry)
        return logEntry
    }

    override fun flush() {}
}

class RemoteMetricLogger(val name: String, val masterURI: URI, val uri: URI, val cutOff: Int = 500) : MetricLogger {
    private var logEntries = CopyOnWriteArrayList<LogEntry>()

    override fun flush() {
        postMessage(masterURI, "/metrics", Klaxon().toJsonString(MetricsResult(uri.host, uri.port, logEntries)))
    }

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
                postMessage(masterURI, "/metrics", Klaxon().toJsonString(MetricsResult(uri.host, uri.port, list)))
            }

            return logEntry
        }
    }
}
