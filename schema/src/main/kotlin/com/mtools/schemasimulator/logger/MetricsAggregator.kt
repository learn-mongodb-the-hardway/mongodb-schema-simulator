package com.mtools.schemasimulator.logger

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.mtools.schemasimulator.stats.Statistics
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import java.util.concurrent.ConcurrentHashMap

class MetricsAggregator {
    private val metrics: MutableMap<Long, ConcurrentHashMap<String, Statistics>> = ConcurrentHashMap()
    private val totalKey = "total"

    val keys: Set<Long>
        get() = metrics.keys

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

            // Add the total
            if (logEntry.total > 0) {
                metrics[logEntry.tick]!![totalKey]!!.addValue(logEntry.total.toDouble())
            }

            val logEntryEntries = logEntry.entries.toTypedArray().copyOf()

            // For each entry add new stat
            for (entry in logEntryEntries) {
                if (!metrics[logEntry.tick]!!.containsKey(entry.first)) {
                    metrics[logEntry.tick]!![entry.first] = Statistics()
                }

                if (entry.second > 0) {
                    metrics[logEntry.tick]!![entry.first]!!.addValue(entry.second.toDouble())
                }
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

    fun entries(key: Long): Map<String, Statistics> {
        return metrics.getValue(key)
    }

    fun aggregate(label: String, skipTicks: Int = 0): DescriptiveStatistics {
        val descriptiveStatistics = DescriptiveStatistics()

        metrics.forEach { key, map ->
            if (key >= skipTicks) {
                if (map.containsKey(label)) {
                    map.getValue(label).values.forEach {
                        if (it > 0.0) {
                            descriptiveStatistics.addValue(it)
                        }
                    }
                }
            }
        }

        return descriptiveStatistics
    }
}
