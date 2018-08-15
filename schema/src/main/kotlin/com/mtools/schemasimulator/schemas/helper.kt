package com.mtools.schemasimulator.schemas

import com.github.javafaker.Faker
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal

@DslMarker
annotation class HelperMarker

@HelperMarker
sealed class Helper<T> {
    abstract fun build(): T
}

interface Field {}

class PrimitiveField(val name: String, val type: Type, val generator: Generator): Field

class Document(val name: String? = null, val fields: MutableList<Field> = mutableListOf()): Field {
    fun add(field: Field) {
        fields += field
    }
}

class DocumentArray(val name: String, val fields: MutableList<Field> = mutableListOf()): Field {
    fun add(field: Field) {
        fields += field
    }
}

fun template(init: DocumentTemplateHelper.() -> Unit): DocumentTemplate {
    val b = DocumentTemplateHelper()
    b.init()
    return b.build()
}

interface Generator {
    fun generate() : Any
}

sealed class BasicStringGenerator : Generator

class FullNameGenerator : BasicStringGenerator() {
    override fun generate() : Any {
        return Faker().name().fullName()
    }
}

class DoubleGenerator(val maxNumberOfDecimals: Int = 2, val min: Int = 0, val max: Int = Int.MAX_VALUE) : Generator {
    override fun generate() : Any {
        return Faker().number().randomDouble(maxNumberOfDecimals, min, max)
    }
}

class ObjectIdGenerator : Generator {
    override fun generate() : Any {
        return ObjectId()
    }
}

class Decimal128TypeGenerator(val min: Double = Double.MIN_VALUE, val max: Double = Double.MAX_VALUE) : Generator {
    override fun generate(): Any {
        return Decimal128(BigDecimal((Faker().commerce().price(min, max))))
    }
}

class PrimitiveFieldHelper(val name: String, val type: Type): Helper<PrimitiveField>() {
    var generator: Generator? = null

    override fun build(): PrimitiveField {
        if (generator == null) {
            generator = when (type) {
                ObjectIdType -> ObjectIdGenerator()
                DecimalType -> Decimal128TypeGenerator()
                else -> FullNameGenerator()
            }
        }

        return PrimitiveField(name, type, generator!!)
    }

    fun fullName() {
       generator = FullNameGenerator()
    }

    fun double(maxNumberOfDecimals: Int = 2, min: Int = 0, max: Int = Int.MAX_VALUE) {
        generator = DoubleGenerator(maxNumberOfDecimals, min, max)
    }

    fun price(min: Double = 0.0, max: Double = Double.MAX_VALUE) {
        generator = Decimal128TypeGenerator(min, max)
    }
}

class DocumentTemplateHelper: Helper<DocumentTemplate>() {
    val document = Document()

    override fun build(): DocumentTemplate {
        return DocumentTemplate(document)
    }

    fun field(name: String, type: Type, init: PrimitiveFieldHelper.() -> Unit = {}) {
        val b = PrimitiveFieldHelper(name, type)
        b.init()
        document.add(b.build())
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper(name)
        b.init()
        document.add(b.build())
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper(name)
        b.init()
        document.add(b.build())
    }
}

class DocumentHelper(val name: String) : Helper<Document>() {
    val fields = mutableListOf<Field>()

    override fun build(): Document {
        return Document(name, fields)
    }

    fun field(name: String, type: Type, init: PrimitiveFieldHelper.() -> Unit) {
        val b = PrimitiveFieldHelper(name, type)
        b.init()
        fields += b.build()
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper(name)
        b.init()
        fields += b.build()
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper(name)
        b.init()
        fields += b.build()
    }
}

class ArrayHelper(val name: String): Helper<DocumentArray>() {
    val fields = mutableListOf<Field>()

    override fun build(): DocumentArray {
        return DocumentArray(name, fields)
    }

    fun field(name: String, type: Type, init: PrimitiveFieldHelper.() -> Unit) {
        val b = PrimitiveFieldHelper(name, type)
        b.init()
        fields += b.build()
    }

    fun arrayOf(name: String, init: ArrayHelper.() -> Unit = {}) {
        val b = ArrayHelper(name)
        b.init()
        fields += b.build()
    }

    fun documentOf(name: String, init: DocumentHelper.() -> Unit = {}) {
        val b = DocumentHelper(name)
        b.init()
        fields += b.build()
    }
}

