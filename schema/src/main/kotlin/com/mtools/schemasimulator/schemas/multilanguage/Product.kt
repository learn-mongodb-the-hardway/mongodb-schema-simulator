package com.mtools.schemasimulator.schemas.multilanguage

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.Decimal128
import java.math.BigDecimal

/*
 * Create a new category instance
 */
class Product(
    logEntry: LogEntry,
    private val products: MongoCollection<Document>,
    val id: Any,
    var name: String,
    var cost: BigDecimal,
    var currency: String,
    var categories: List<Document>
) : Scenario(logEntry) {
    override fun indexes(): List<Index> = listOf(
        Index(products.namespace.databaseName, products.namespace.collectionName,
            Indexes.ascending("categories._id")
        )
    )

    /*
     * Create a new mongodb product createWriteModel
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
    fun reload() = log("reload") {
        val product = products.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        product ?: throw SchemaSimulatorException("could not locate category with id $id")
        val _cost = product["cost"] as Decimal128

        name = product.getString("name")
        cost = _cost.bigDecimalValue()
        currency = product.getString("currency")
        @Suppress("UNCHECKED_CAST")
        categories = product["categories"] as List<Document>
    }
}
