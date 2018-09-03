package com.mtools.schemasimulator.schemas

import com.mongodb.client.model.IndexOptions
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.conversions.Bson
import kotlin.system.measureNanoTime

interface ActionValues

interface IndexClass {
    val indexes: List<Index>
}

data class Index(val db: String, val collection: String, val keys: Bson, val options: IndexOptions = IndexOptions())

abstract class Action(protected val logEntry: LogEntry) {
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

class SchemaSimulatorException(message: String): Exception(message)
