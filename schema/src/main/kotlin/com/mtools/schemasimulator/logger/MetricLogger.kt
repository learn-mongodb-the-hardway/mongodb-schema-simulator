package com.mtools.schemasimulator.logger

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.messages.worker.MetricsResult
import mu.KLogging
import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
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

class LocalMetricLogger(
    private val name: String,
    private val aggregator: MetricsAggregator,
    private val cutOff: Int = 500
) : MetricLogger {
    private var logEntries = ConcurrentLinkedQueue<LogEntry>()

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        synchronized(this) {
            val logEntry = LogEntry(simulation, tick)
            logEntries.add(logEntry)

            if (logEntries.size == cutOff) {
                // Get a reference
                val list = logEntries.toTypedArray().copyOf()
                // Empty the list
                logEntries = ConcurrentLinkedQueue()
                // Send a metrics message
                aggregator.processTicks(list.toList())
                // Log metrics sent
                logger.info("metrics received [$name]:[Max Tick:${list.map {
                    it.tick
                }.max()}]")
            }

            return logEntry
        }
    }

    override fun flush() {
        // Get a reference
        val list = logEntries.toList()
        // Empty the list
        // Empty the list
        synchronized(logEntries) {
            logEntries = ConcurrentLinkedQueue()
        }
        // Process remaining ticks
        aggregator.processTicks(list)
    }

    companion object : KLogging()
}

class RemoteMetricLogger(val name: String, val masterURI: URI, val uri: URI, val cutOff: Int = 500) : MetricLogger {
    private var logEntries = CopyOnWriteArrayList<LogEntry>()

    override fun flush() {
        // Get a reference
        val list = logEntries
        // Empty the list
        logEntries = CopyOnWriteArrayList()
        // Send message
        postMessage(masterURI, "/metrics", Klaxon().toJsonString(MetricsResult(uri.host, uri.port, list)))
    }

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        synchronized(this) {
            val logEntry = LogEntry(simulation, tick)
            logEntries.add(logEntry)

            if (logEntries.size == cutOff) {
                // Get a reference
                val list = logEntries.toTypedArray().copyOf().toList()
                // Empty the list
                logEntries = CopyOnWriteArrayList()
                // Send a metrics message
                postMessage(masterURI, "/metrics", Klaxon().toJsonString(MetricsResult(uri.host, uri.port, list)))
            }

            return logEntry
        }
    }
}
