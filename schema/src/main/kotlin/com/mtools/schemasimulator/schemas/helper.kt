package com.mtools.schemasimulator.schemas

import com.github.javafaker.Faker
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal
import java.util.*

@DslMarker
annotation class HelperMarker

@HelperMarker
sealed class Helper<T> {
    abstract fun build(): T
}

interface Field {
    val name: String?
    operator fun get(name: String): Field?
}

class PrimitiveField(override val name: String, val type: Type, val generator: Generator): Field {
    override operator fun get(name: String): Field? {
        return null
    }
}

class Document(override val name: String? = null, val fields: MutableList<Field> = mutableListOf()): Field {
    operator fun plus(field: Field) {
        fields += field
    }

    operator fun plusAssign(field: Field) {
        fields += field
    }

    override operator fun get(name: String): Field? {
        return fields.firstOrNull { it.name == name}
    }
}

class DocumentArray(override val name: String, private val fields: MutableList<Field> = mutableListOf()): Field {
    operator fun plus(field: Field) {
        fields += field
    }

    operator fun plusAssign(field: Field) {
        fields += field
    }

    override operator fun get(name: String): Field? {
        return fields.firstOrNull { it.name == name}
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

class DoubleGenerator(
    private val maxNumberOfDecimals: Int = 2,
    private val min: Int = 0,
    private val max: Int = Int.MAX_VALUE) : Generator {

    override fun generate() : Any {
        return Faker().number().randomDouble(maxNumberOfDecimals, min, max)
    }
}

class IntegerGenerator(
    private val min: Int = 0,
    private val max: Int = Int.MAX_VALUE) : Generator {

    override fun generate() : Any {
        return Faker().number().numberBetween(min, max)
    }
}

class ObjectIdGenerator : Generator {
    override fun generate() : Any {
        return ObjectId()
    }
}

class Decimal128TypeGenerator(
    private val min: Double = Double.MIN_VALUE,
    private val max: Double = Double.MAX_VALUE) : Generator {

    override fun generate(): Any {
        return Decimal128(BigDecimal((Faker().commerce().price(min, max))))
    }
}

class DateTypeGenerator() : Generator {
    override fun generate(): Any {
        return Date()
    }
}

class PrimitiveFieldHelper(
    private val name: String,
    private val type: Type,
    private var generator: Generator?): Helper<PrimitiveField>() {

    override fun build(): PrimitiveField {
        if (generator == null) {
            generator = when (type) {
                ObjectIdType -> ObjectIdGenerator()
                DecimalType -> Decimal128TypeGenerator()
                DateType -> DateTypeGenerator()
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

sealed class FieldCollectionHelper<T> : Helper<T>() {
    val fields = mutableListOf<Field>()

    fun field(name: String, type: Type, generator: Generator? = null, init: PrimitiveFieldHelper.() -> Unit = {}) {
        val b = PrimitiveFieldHelper(name, type, generator)
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

class DocumentTemplateHelper: FieldCollectionHelper<DocumentTemplate>() {
    override fun build(): DocumentTemplate {
        return DocumentTemplate(Document(null, fields))
    }
}

class DocumentHelper(val name: String) : FieldCollectionHelper<Document>() {
    override fun build(): Document {
        return Document(name, fields)
    }
}

class ArrayHelper(val name: String): FieldCollectionHelper<DocumentArray>() {
    override fun build(): DocumentArray {
        return DocumentArray(name, fields)
    }
}

