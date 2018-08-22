package com.mtools.schemasimulator.schemas

interface DataGeneratorOptions

interface DataGenerator {
    fun generate(options: DataGeneratorOptions)
}

class DocumentTemplate(val document: Document)

