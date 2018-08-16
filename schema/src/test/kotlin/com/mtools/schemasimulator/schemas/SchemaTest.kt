package com.mtools.schemasimulator.schemas

import org.junit.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

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

        template.shouldContainField("_id", ObjectIdType, ObjectIdGenerator())
        template.shouldContainField("name", StringType, FullNameGenerator())
        template.shouldContainField("price", DecimalType, Decimal128TypeGenerator())
    }

    @Test
    fun renderStructuredTemplate() {
        val template = template {
            field("_id", ObjectIdType)

            documentOf("doc") {
                field("text", StringType)

                documentOf("doc") {
                    field("text", StringType)
                }

                arrayOf("array") {
                    field("text", StringType)
                }
            }
        }

        template.shouldContainDocument("doc")
        template.shouldContainDocument("doc.doc")
        template.shouldContainArray("doc.array")
        template.shouldContainField("_id", ObjectIdType, ObjectIdGenerator())
        template.shouldContainField("doc.text", StringType, FullNameGenerator())
        template.shouldContainField("doc.doc.text", StringType, FullNameGenerator())
        template.shouldContainField("doc.array.text", StringType, FullNameGenerator())
    }
}

private fun DocumentTemplate.shouldContainArray(name: String) {
    var field: Field? = this.document

    name.split(".").forEach {
        field = field!![it]
    }

    assertNotNull(field, "could not locate field [$name]")
    assertTrue(field is DocumentArray, "field was not a Document instance")
}

private fun DocumentTemplate.shouldContainDocument(name: String) {
    var field: Field? = this.document

    name.split(".").forEach {
        field = field!![it]
    }

    assertNotNull(field, "could not locate field [$name]")
    assertTrue(field is Document, "field was not a DocumentArray instance")
}

private fun DocumentTemplate.shouldContainField(name: String, type: Type, generator: Generator?) {
    var field: Field? = this.document

    name.split(".").forEach {
        field = field!![it]
    }

    assertNotNull(field, "could not locate field [$name]")
    assertTrue(field is PrimitiveField, "field was not a PrimitiveField instance")
    val primitiveField = field as PrimitiveField
    assertTrue(primitiveField.type.javaClass.name == type.javaClass.name)

    if (generator != null) {
        assertTrue(primitiveField.generator.javaClass.name == generator.javaClass.name)
    }
}
