package com.mtools.schemasimulator.schemas

interface DataGenerator {
    fun generate(options: Map<String, Any>)
}

class DocumentTemplate(val document: Document)

