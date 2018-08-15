package com.mtools.schemasimulator.schemas

@DslMarker
annotation class HelperMarker

@HelperMarker
sealed class Helper<T> {
    abstract fun build(): T
}

interface Field {}

class PrimitiveField(val name: String, val type: Type): Field

class Document(val fields: MutableList<Field> = mutableListOf()): Field {
    fun add(field: Field) {
        fields += field
    }
}

class DocumentArray(val fields: MutableList<Field> = mutableListOf()): Field {
    fun add(field: Field) {
        fields += field
    }
}

fun template(init: DocumentTemplateHelper.() -> Unit): DocumentTemplate {
    val b = DocumentTemplateHelper()
    b.init()
    return b.build()
}

class DocumentTemplateHelper: Helper<DocumentTemplate>() {
    val document = Document()

    override fun build(): DocumentTemplate {
        return DocumentTemplate(document)
    }

    fun field(name: String, type: Type) {
        document.add(PrimitiveField(name, type))
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper()
        b.init()
        document.add(b.build())
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper()
        b.init()
        document.add(b.build())
    }
}

class DocumentHelper : Helper<Document>() {
    val fields = mutableListOf<Field>()

    override fun build(): Document {
        return Document(fields)
    }

    fun field(name: String, type: Type) {
        fields += PrimitiveField(name, type)
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper()
        b.init()
        fields += b.build()
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper()
        b.init()
        fields += b.build()
    }
}

class ArrayHelper: Helper<DocumentArray>() {
    val fields = mutableListOf<Field>()

    override fun build(): DocumentArray {
        return DocumentArray(fields)
    }

    fun field(name: String, type: Type) {
        fields += PrimitiveField(name, type)
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper()
        b.init()
        fields += b.build()
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper()
        b.init()
        fields += b.build()
    }
}

