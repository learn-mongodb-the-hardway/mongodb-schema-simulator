package com.mtools.schemasimulator.schemas

interface DataGenerator {
    fun generate(options: Map<String, Any> = mapOf())
}

class DocumentTemplate(val document: Document)

