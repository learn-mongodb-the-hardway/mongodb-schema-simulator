package com.mtools.schemasimulator.schemas.shoppingcartnoreservation

import com.github.javafaker.Faker
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.schemas.DataGenerator
import com.mtools.schemasimulator.schemas.DataGeneratorOptions
import com.mtools.schemasimulator.schemas.DateType
import com.mtools.schemasimulator.schemas.DocumentTemplate
import com.mtools.schemasimulator.schemas.DoubleGenerator
import com.mtools.schemasimulator.schemas.DoubleType
import com.mtools.schemasimulator.schemas.Generator
import com.mtools.schemasimulator.schemas.IntegerGenerator
import com.mtools.schemasimulator.schemas.IntegerType
import com.mtools.schemasimulator.schemas.ObjectIdType
import com.mtools.schemasimulator.schemas.PrimitiveField
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import com.mtools.schemasimulator.schemas.StringType
import com.mtools.schemasimulator.schemas.template
import org.bson.Document

class ShoppingCartDataGeneratorOptions(
    val numberOfDocuments: Int, val minimumInventoryQuantity: Int) : DataGeneratorOptions

class ShoppingCartDataGenerator(private val db: MongoDatabase): DataGenerator {
    override fun generate(options: DataGeneratorOptions) {
        val maxDocumentsInBatch = 1000

        // Ensure we have the right type of options
        if (!(options is ShoppingCartDataGeneratorOptions)) {
            throw SchemaSimulatorException("options object must be of type ShoppingCartDataGeneratorOptions")
        }

        // Collections
        val productCollection = db.getCollection("products")
        val inventoryCollection = db.getCollection("inventories")
        val products = mutableListOf<Document>()
        val inventories = mutableListOf<Document>()

        // Generate the numberOfExpected product documents
        for (i in 0 until options.numberOfDocuments) {
            products += DocumentGenerator(template {
                field("_id", ObjectIdType)
                field("name", StringType, ProductNameGenerator())
                field("price", DoubleType, DoubleGenerator())
            }).forEach {
                inventories += DocumentGenerator(template {
                    field("_id", ObjectIdType)
                    field("modifiedOn", DateType)
                    field("quantity", IntegerType.INT32, IntegerGenerator(min = options.minimumInventoryQuantity))
                }).generate(mapOf("_id" to it["_id"]!!))
            }.generate()

            insertDocumentsList(products, maxDocumentsInBatch, productCollection)
            insertDocumentsList(inventories, maxDocumentsInBatch, inventoryCollection)
        }

        // Insert leftover documents
        if (products.size > 0) productCollection.insertMany(products)
        if (inventories.size > 0) inventoryCollection.insertMany(inventories)
    }

    private fun insertDocumentsList(list: MutableList<Document>, maxDocumentsInBatch: Int, collection: MongoCollection<Document>) {
        if (list.size == maxDocumentsInBatch) {
            collection.insertMany(list)
            list.clear()
        }
    }
}

private class ProductNameGenerator: Generator {
    override fun generate(): Any {
        return Faker().lorem().sentence(3)
    }
}

class DocumentGenerator(private val template: DocumentTemplate) {
    private val functions = mutableListOf<(document:Document) -> Unit>()

    fun generate(map: Map<String, Any> = mapOf()): Document {
        val document = Document()

        // For all fields generate the docucment fields
        template.document.fields.forEach {
            when (it) {
                is PrimitiveField -> {
                    if (map.containsKey(it.name)) {
                        document.append(it.name, map.getValue(it.name))
                    } else {
                        document.append(it.name, it.generator.generate())
                    }
                }
            }
        }

        functions.forEach {
            it.invoke(document)
        }
        return document
    }

    fun forEach(function: (document:Document) -> Unit) : DocumentGenerator {
        functions += function
        return this
    }
}
