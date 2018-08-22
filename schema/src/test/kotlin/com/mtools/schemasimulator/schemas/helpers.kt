package com.mtools.schemasimulator.schemas

import org.bson.Document
import java.util.ArrayList
import kotlin.test.assertEquals
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
