package com.mtools.schemasimulator.schemas

import com.mtools.schemasimulator.logger.LogEntry
import kotlin.system.measureNanoTime

interface ActionValues

abstract class Action(val logEntry: LogEntry) {
    abstract fun run(values: ActionValues) : Map<String, Any>

    fun execute(values: ActionValues) : Map<String, Any> {
        var results: Map<String, Any> = mapOf()

        val time = measureNanoTime {
            results = run(values)
        }

        logEntry.add(this.javaClass.simpleName, time)
        return results
    }
}