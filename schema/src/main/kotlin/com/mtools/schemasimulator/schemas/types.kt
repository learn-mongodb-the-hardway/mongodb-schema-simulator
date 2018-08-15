package com.mtools.schemasimulator.schemas

sealed class Type

data class BinaryType(val subtype: Int) : Type() {
    override fun toString(): String {
        return "BINARY"
    }
}

object BooleanType : Type() {
    override fun toString(): String {
        return "BOOLEAN"
    }
}

object DateType : TemporalType() {
    override fun toString(): String {
        return "DATE"
    }
}

object TimestampType : TemporalType() {
    override fun toString(): String {
        return "DATE"
    }
}

object DecimalType : NumberType() {
    override fun toString(): String {
        return "DECIMAL"
    }
}

object DoubleType : NumberType() {
    override fun toString(): String {
        return "DOUBLE"
    }
}

data class IntegerType(val bitness: Int, val isUnsigned: Boolean = false) : NumberType() {
    override fun toString(): String {
        return String.format("INT%d", bitness)
    }

    companion object {
        val INT32 = IntegerType(32)
        val INT64 = IntegerType(64)
    }
}

sealed class NumberType : Type()

object StringType : Type() {
    override fun toString(): String {
        return "STRING"
    }
}

sealed class TemporalType : Type()

object UnknownType : Type() {
    override fun toString(): String {
        return "UNKNOWN"
    }
}

object NullType : Type() {
    override fun toString(): String {
        return "NULL"
    }
}

object ObjectIdType : Type() {
    override fun toString(): String {
        return "OBJECTID"
    }
}

object DBPointerType : Type() {
    override fun toString(): String {
        return "DBPOINTER"
    }
}

class JavaScriptType(val scope: Boolean = false) : Type() {
    override fun toString(): String {
        return if (!scope) "JAVASCRIPT" else "JAVASCRIPT(SCOPE)"
    }
}

object MinKeyType : Type() {
    override fun toString(): String {
        return "MINKEY"
    }
}

object MaxKeyType : Type() {
    override fun toString(): String {
        return "MAXKEY"
    }
}
