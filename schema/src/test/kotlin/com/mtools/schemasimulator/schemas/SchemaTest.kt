package com.mtools.schemasimulator.schemas

import com.github.javafaker.Faker
import org.junit.Test

class SchemaTest {
    @Test
    fun renderSimpleTemplate() {
        val template = template {
            field("_id", ObjectIdType)
            field("name", StringType) {
                fullName()
            }
            field("price", DecimalType)
        }

        println()
    }
}
