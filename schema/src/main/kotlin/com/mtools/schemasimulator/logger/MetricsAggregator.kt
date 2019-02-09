package com.mtools.schemasimulator.logger

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.mtools.schemasimulator.stats.Statistics
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import java.util.concurrent.ConcurrentHashMap

class MetricsAggregator {
    val metrics: MutableMap<Long, ConcurrentHashMap<String, Statistics>> = ConcurrentHashMap()
//    private val metricsByType: MutableMap<String, Statistics> = ConcurrentHashMap()
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

//            // Add a statistics entry
//            if (!metricsByType.containsKey(totalKey)) {
//                metricsByType[totalKey] = Statistics()
//            }

            // Add the total
            metrics[logEntry.tick]!![totalKey]!!.addValue(logEntry.total.toDouble())
//            metricsByType["total"]!!.addValue(logEntry.total.toDouble())

            val logEntryEntries = logEntry.entries.toTypedArray().copyOf()

            // For each entry add new stat
            for (entry in logEntryEntries) {
                if (!metrics[logEntry.tick]!!.containsKey(entry.first)) {
                    metrics[logEntry.tick]!![entry.first] = Statistics()
                }

//                // Add a statistics entry
//                if (!metricsByType.containsKey(entry.first)) {
//                    metricsByType[entry.first] = Statistics()
//                }

                metrics[logEntry.tick]!![entry.first]!!.addValue(entry.second.toDouble())
//                metricsByType[entry.first]!!.addValue(entry.second.toDouble())
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
                        descriptiveStatistics.addValue(it)
                    }
                }
            }
        }

        return descriptiveStatistics
    }
}
