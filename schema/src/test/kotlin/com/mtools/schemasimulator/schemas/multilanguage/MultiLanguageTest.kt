package com.mtools.schemasimulator.schemas.multilanguage

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.createLogEntry
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MultiLanguageTest {
    @BeforeEach
    fun beforeEach() {
        products.drop()
        categories.drop()
    }

    @Test
    @DisplayName("Correctly add new local for a category and see it reflected in the products")
    fun test1() {
        setupCategories(listOf(
            CatEntry(1, Document(mapOf("en-us" to "car", "de-de" to "auto")))
        ))

        // Get all the categories
        val cats = categories.find().toList()

        // Create a product
        val product = Product(createLogEntry(), products, 1, "car", BigDecimal(100), "usd", cats)
        product.create()

        // Let's attempt to add a local to the category
        val cat = Category(createLogEntry(), categories, products, 1)
        cat.addLocal("es-es", "coche")

        // Reload the product
        product.reload()
        val doc = product.categories[0].get("names") as Document
        assertEquals("coche", doc.getString("es-es"))

        // Reload the category
        cat.reload()
        assertEquals("coche", cat.names.getString("es-es"))
    }

    @Test
    @DisplayName("Correctly remove a local for a category and see it reflected in the products")
    fun test2() {
        setupCategories(listOf(
            CatEntry(1, Document(mapOf("en-us" to "car", "de-de" to "auto")))
        ))

        // Get all the categories
        val cats = categories.find().toList()

        // Create a product
        val product = Product(createLogEntry(), products, 1, "car", BigDecimal(100), "usd", cats)
        product.create()

        // Let's attempt to add a local to the category
        val cat = Category(createLogEntry(), categories, products, 1)
        cat.removeLocal("de-de")

        // Reload the product
        product.reload()
        val doc = product.categories[0].get("names") as Document
        assertNull(doc["de-de"])

        // Reload the category
        cat.reload()
        assertNull(cat.names["de-de"])
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var products: MongoCollection<Document>
        lateinit var categories: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            products = db.getCollection("products")
            categories = db.getCollection("categories")

            // Drop collection
            products.drop()
            categories.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }

    data class CatEntry(val id: Any, val doc: Document)

    fun setupCategories(cats: List<CatEntry>) {
        cats.forEach {
            Category(createLogEntry(), categories, products, it.id, it.doc).create()
        }
    }
}
