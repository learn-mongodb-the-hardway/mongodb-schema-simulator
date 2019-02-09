package com.mtools.schemasimulator.logger

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.mtools.schemasimulator.stats.Statistics
import java.util.concurrent.ConcurrentHashMap

class MetricsAggregator {
    val metrics: MutableMap<Long, ConcurrentHashMap<String, Statistics>> = ConcurrentHashMap()
    val metricsByType: MutableMap<String, Statistics> = ConcurrentHashMap()
    val totalKey = "total"

    @Synchronized
    fun processTicks(logEntries: List<LogEntry>) {
        val entries = logEntries.toTypedArray().copyOf()
        for (logEntry in entries) {
            if (!metrics.containsKey(logEntry.tick)) {
                metrics[logEntry.tick] = ConcurrentHashMap()
            }

            // Create new statistics entry
            if (!metrics[logEntry.tick]!!.containsKey(totalKey)) {
                metrics[logEntry.tick]!![totalKey] = Statistics(10000)
            }

            // Add a statistics entry
            if (!metricsByType.containsKey(totalKey)) {
                metricsByType[totalKey] = Statistics()
            }

            // Add the total
            metrics[logEntry.tick]!![totalKey]!!.addValue(logEntry.total.toDouble() / 1000000)
            metricsByType["total"]!!.addValue(logEntry.total.toDouble() / 1000000)

            val logEntryEntries = logEntry.entries.toTypedArray().copyOf()

            // For each entry add new stat
            for (entry in logEntryEntries) {
                if (!metrics[logEntry.tick]!!.containsKey(entry.first)) {
                    metrics[logEntry.tick]!![entry.first] = Statistics()
                }

                // Add a statistics entry
                if (!metricsByType.containsKey(entry.first)) {
                    metricsByType[entry.first] = Statistics()
                }

                metrics[logEntry.tick]!![entry.first]!!.addValue(entry.second.toDouble() / 1000000)
                metricsByType[entry.first]!!.addValue(entry.second.toDouble() / 1000000)
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
