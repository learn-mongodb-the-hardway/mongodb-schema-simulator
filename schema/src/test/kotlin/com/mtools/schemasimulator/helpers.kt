package com.mtools.schemasimulator

import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.messages.worker.MetricsResult
import org.bson.Document
import java.util.ArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

data class f(val value: Any? = Skip(), val klass: Any? = Skip(), val isNull: Boolean = false, val listSize: Int? = null) {
    class Skip
}

fun Document.shouldContainValues(values: Map<String, Any>) {
    values.forEach { path, fieldValue ->
        val field = g(path)

        when (fieldValue) {
            is f -> {
                // Deal with field value comparision
                if (fieldValue.value != null
                    && fieldValue.value::class != f.Skip::class) {
                    assertEquals(fieldValue.value, field, "For field $path the expected value ${fieldValue.value} does not match the value $field")
                }

                // Deal with field type comparison
                if (fieldValue.klass != null
                    && fieldValue.klass::class != f.Skip::class) {
                    assertEquals(fieldValue.klass::class.qualifiedName, field::class.qualifiedName, "For field $path the expected type ${fieldValue.klass::class.qualifiedName} does not match encountered type ${field::class.qualifiedName}")
                }

                if (fieldValue.listSize != null && fieldValue.klass is ArrayList<*> && field is ArrayList<*>) {
                    assertEquals(fieldValue.listSize, field.size)
                }

                // Check nullability of field
                if (!fieldValue.isNull) {
                    assertNotNull(field, "field at path was null, expected not null")
                }
            }
            else -> {
                assertEquals(fieldValue, field, "For field $path the expected value $fieldValue does not match $field")
            }
        }
    }
}

fun Document.shouldNoContainFields(paths: List<String>) {
    paths.forEach { path ->
        assertFalse { exists(path) }
    }
}

fun Document.g(path: String) : Any {
    var field: Any = this

    for(part in path.split(".")) {
        field = when (field) {
            is Document -> field[part]!!
            is ArrayList<*> -> field[part.toInt()]
            is Array<*> -> field[part.toInt()]!!
            else -> field
        }
    }

    return field
}

fun Document.exists(path: String) : Boolean {
    var field: Any? = this

    for(part in path.split(".")) {
        if (field == null) return false

        field = when (field) {
            is Document -> field[part]
            is ArrayList<*> -> field[part.toInt()]
            is Array<*> -> field[part.toInt()]
            else -> field
        }
    }

    return field != null
}

class TestMetricLogger(): MetricLogger {
    override fun toMetricResult(): MetricsResult {
        return MetricsResult(listOf())
    }

    val measures = mutableMapOf<String, MutableList<LogEntry>>()

    override fun createLogEntry(simulation: String, tick: Long): LogEntry {
        if (!measures.containsKey(simulation)) {
            measures[simulation] = mutableListOf()
        }

        val entry = LogEntry(simulation, tick)
        measures[simulation]?.add(entry)
        return entry
    }
}

fun createLogEntry() : LogEntry {
    return LogEntry("", 0)
}
