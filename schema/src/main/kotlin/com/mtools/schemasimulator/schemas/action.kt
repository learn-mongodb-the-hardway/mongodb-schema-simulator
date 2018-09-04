package com.mtools.schemasimulator.schemas

import com.mongodb.ReadPreference
import com.mongodb.client.model.IndexOptions
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.conversions.Bson
import kotlin.system.measureNanoTime

abstract class Scenario(protected val logEntry: LogEntry) {
    abstract fun indexes(): List<Index>

    // Log
    protected fun log(name: String, block: () -> Unit) {
        logEntry.add("${this.javaClass.simpleName}:$name", measureNanoTime(block))
    }
}

interface AcceptsReadPreference {
    fun setReadPreference(preference: ReadPreference)
}

data class Index(val db: String, val collection: String, val keys: Bson, val options: IndexOptions = IndexOptions())

//abstract class Action(protected val logEntry: LogEntry) {
//    abstract fun run(values: ActionValues) : Map<String, Any>
//
//    fun execute(values: ActionValues) : Map<String, Any> {
//        var results: Map<String, Any> = mapOf()
//
//        val time = measureNanoTime {
//            results = run(values)
//        }
//
//        logEntry.add(this.javaClass.simpleName, time)
//        return results
//    }
//}

class SchemaSimulatorException(message: String): Exception(message)
