package com.mtools.schemasimulator.logger

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import java.util.concurrent.ConcurrentHashMap

class MetricsAggregator {
    val metrics: MutableMap<Long, ConcurrentHashMap<String, SummaryStatistics>> = ConcurrentHashMap()

    @Synchronized
    fun processTicks(logEntries: List<LogEntry>) {
        val entries = logEntries.toTypedArray().copyOf()
        for (logEntry in entries) {
            if (!metrics.containsKey(logEntry.tick)) {
                metrics[logEntry.tick] = ConcurrentHashMap()
            }

            // Create new statistics entry
            if (!metrics[logEntry.tick]!!.containsKey("total")) {
                metrics[logEntry.tick]!!["total"] = SummaryStatistics()
            }

            // Add the total
            metrics[logEntry.tick]!!["total"]!!.addValue(logEntry.total.toDouble() / 1000000)

            val logEntries = logEntry.entries.toTypedArray().copyOf()
            // For each entry add new stat
            for (entry in logEntries) {
                if (!metrics[logEntry.tick]!!.containsKey(entry.first)) {
                    metrics[logEntry.tick]!![entry.first] = SummaryStatistics()
                }

                metrics[logEntry.tick]!![entry.first]!!.addValue(entry.second.toDouble() / 1000000)
            }
        }
    }

    @Synchronized
    fun processTicks(ticks: JsonArray<JsonObject>) {
        ticks.forEach { tick ->
            val simulation = tick.string("name")!!
            val tickNumber = tick.int("tick")!!.toLong()
            val total = tick.int("total")!!.toLong()

            val entries = tick.array<JsonObject>("entries")!!.map { entry ->
                val scenario = entry.string("first")!!
                val value = entry.int("second")!!.toLong()
                Pair(scenario, value)
            }

            processTicks(listOf(LogEntry(simulation, tickNumber, entries.toMutableList(), total)))
        }
    }
}
