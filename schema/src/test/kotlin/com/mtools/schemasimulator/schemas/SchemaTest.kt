package com.mtools.schemasimulator.schemas

import org.junit.Test

class SchemaTest {
    @Test
    fun renderSimpleTemplate() {
        val template = template {
            field("_id", ObjectIdType)
            field("name", StringType)
            field("price", DecimalType)
        }

        println()
    }
}
