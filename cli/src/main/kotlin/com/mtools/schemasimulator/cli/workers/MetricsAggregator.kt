package com.mtools.schemasimulator.cli.workers

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.mtools.schemasimulator.logger.LogEntry
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import java.util.concurrent.ConcurrentHashMap

class MetricsAggregator {
    val metrics: MutableMap<Long, MutableMap<String, SummaryStatistics>> = ConcurrentHashMap()

    fun processTicks(logEntries: List<LogEntry>) {
        logEntries.forEach { logEntry ->
            if (!metrics.containsKey(logEntry.tick)) {
                metrics[logEntry.tick] = mutableMapOf()
            }

            // Create new statistics entry
            if (!metrics[logEntry.tick]!!.containsKey("total")) {
                metrics[logEntry.tick]!!["total"] = SummaryStatistics()
            }

            // Add the total
            metrics[logEntry.tick]!!["total"]!!.addValue(logEntry.total.toDouble() / 1000000)

            // For each entry add new stat
            logEntry.entries.forEach {
                if (!metrics[logEntry.tick]!!.containsKey(it.first)) {
                    metrics[logEntry.tick]!![it.first] = SummaryStatistics()
                }

                metrics[logEntry.tick]!![it.first]!!.addValue(it.second.toDouble() / 1000000)
            }
        }
    }

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
