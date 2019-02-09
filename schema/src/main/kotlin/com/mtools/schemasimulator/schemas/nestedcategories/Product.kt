package com.mtools.schemasimulator.schemas.nestedcategories

import com.mongodb.ReadPreference
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.AcceptsReadPreference
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal

/*
 * Create a new category instance
 */
class Product(
    logEntry: LogEntry,
    private val products: MongoCollection<Document>,
    private val categoriesCollection: MongoCollection<Document>,
    val id: Any = ObjectId(),
    var name: String = "",
    var cost: BigDecimal = BigDecimal.ZERO,
    var currency: String = "",
    var categories: List<String> = listOf()
) : Scenario(logEntry), AcceptsReadPreference {
    private var readPreference: ReadPreference = ReadPreference.primary()

    override fun setReadPreference(preference: ReadPreference) {
        readPreference = preference
    }

    override fun indexes(): List<Index> = listOf(
        Index(products.namespace.databaseName, products.namespace.collectionName,
            Indexes.ascending("categories")
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
        val category = products.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        category ?: throw SchemaSimulatorException("could not locate category with id $id")

        name = category.getString("name")
        cost = category["cost"] as BigDecimal
        currency = category.getString("currency")
        @Suppress("UNCHECKED_CAST")
        categories = category["categories"] as List<String>
    }

    /*
     * Find all products for a specific category
     */
    fun findByCategory(path: String) : List<Product> {
        var results = listOf<Product>()

        log ("findByCategory") {
            results = products
                .withReadPreference(readPreference)
                .find(Document(mapOf(
                    "categories" to path)))
                .map {
                    Product(
                        logEntry,
                        products,
                        categoriesCollection,
                        it["_id"]!!,
                        it.getString("name"),
                        (it["cost"]!! as Decimal128).bigDecimalValue(),
                        it.getString("currency"),
                        @Suppress("UNCHECKED_CAST")
                        it["categories"] as List<String>
                    )
                }
                .toList()
        }

        return results
    }

    /*
     * Find all products for a categories direct children
     */
    fun findByDirectCategoryChildren(path: String, allowUsageOfCoveredIndex: Boolean = false) : List<Product> {
        var results = listOf<Product>()

        log ("findByDirectCategoryChildren") {
            // Locate all the categories
            val categories = Category(logEntry, categoriesCollection, products)
                .findAllDirectChildCategories(path, allowUsageOfCoveredIndex)
            // Convert to paths
            val paths = categories.map { it.category }
            // Get all the products
            results = products
                .withReadPreference(readPreference)
                .find(Document(mapOf(
                    "categories" to mapOf(
                        "\$in" to paths
                    )
                )))
                .map {
                    Product(
                        logEntry,
                        products,
                        categoriesCollection,
                        it["_id"]!!,
                        it.getString("name"),
                        (it["cost"]!! as Decimal128).bigDecimalValue(),
                        it.getString("currency"),
                        @Suppress("UNCHECKED_CAST")
                        it["categories"] as List<String>
                    )
                }
                .toList()
        }

        return results
    }

    /*
     * Find all products for a specific category tree
     */
    fun findByCategoryTree(path: String, allowUsageOfCoveredIndex: Boolean = false) : List<Product> {
        var results = listOf<Product>()

        log ("findByCategoryTree") {
            // Locate all the categories
            val categories = Category(logEntry, categoriesCollection, products)
                .findAllChildCategories(path, allowUsageOfCoveredIndex)
            // Convert to paths
            val paths = categories.map { it.category }
            // Get all the products
            results = products
                .withReadPreference(readPreference)
                .find(Document(mapOf(
                    "categories" to mapOf(
                        "\$in" to paths
                    )
                )))
                .map {
                    Product(
                        logEntry,
                        products,
                        categoriesCollection,
                        it["_id"]!!,
                        it.getString("name"),
                        (it["cost"]!! as Decimal128).bigDecimalValue(),
                        it.getString("currency"),
                        @Suppress("UNCHECKED_CAST")
                        it["categories"] as List<String>
                    )
                }
                .toList()
        }

        return results
    }
}
