package com.mtools.schemasimulator.schemas.multilanguage

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import java.math.BigDecimal

/*
 * Create a new category instance
 */
class Product(
    logEntry: LogEntry,
    private val products: MongoCollection<Document>,
    private val id: Any,
    private var name: String,
    private var cost: BigDecimal,
    private var currency: String,
    private var categories: Document
) : Scenario(logEntry) {
    override fun indexes(): List<Index> = listOf(
        Index(products.namespace.databaseName, products.namespace.collectionName,
            Indexes.ascending("categories._id")
        )
    )

    /*
     * Create a new mongodb product document
     */
    fun create() = log("create") {
        products.insertOne(Document(mapOf(
            "_id" to id,
            "name" to name,
            "cost" to cost,
            "currency" to currency,
            "categories" to categories
        )))
    }

    /*
     * Reload the product information
     */
    /*
     * Reload the category information
     */
    fun reload() = log("reload") {
        val category = products.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        category ?: throw SchemaSimulatorException("could not locate category with id $id")

        name = category.getString("name")
        cost = category["cost"] as BigDecimal
        currency = category.getString("currency")
        categories = category["categories"] as Document
    }
}
