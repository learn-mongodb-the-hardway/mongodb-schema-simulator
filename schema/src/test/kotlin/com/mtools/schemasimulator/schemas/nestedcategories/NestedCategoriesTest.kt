package com.mtools.schemasimulator.schemas.nestedcategories

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class NestedCategoriesTest {
    @BeforeEach
    internal fun beforeEach() {
        products.drop()
        categories.drop()
    }

    @Test
    @DisplayName("Correctly category and fetch all immediate children of root node")
    fun test1() {
        setupCategories(listOf(
            listOf("root", "/", ""),
            listOf("1", "/1", "/"), listOf("2", "/2", "/"), listOf("3", "/3", "/"),
            listOf("1-1", "/1/1", "/1"), listOf("1-2", "/1/2", "/1"),
            listOf("2-1", "/2/1", "/2"), listOf("2-2", "/2/2", "/2"),
            listOf("3-1", "/3/1", "/3"), listOf("3-2", "/3/2", "/3")
        ))

        val categories = Category(LogEntry(""), categories).findAllDirectChildCategories("/")
        assertEquals(3, categories.size)

        val paths = mutableMapOf("/1" to true, "/2" to true, "/3" to true)
        categories.forEach {
            paths.remove(it.category)
        }

        assertEquals(0, paths.size)
    }

    @Test
    @DisplayName("Correctly fetch Category tree under a specific path")
    fun test2() {
        setupCategories(listOf(
            listOf("root", "/", ""),
            listOf("1", "/1", "/"), listOf("2", "/2", "/"), listOf("3", "/3", "/"),
            listOf("1-1", "/1/1", "/1"), listOf("1-2", "/1/2", "/1"),
            listOf("2-1", "/2/1", "/2"), listOf("2-2", "/2/2", "/2"),
            listOf("3-1", "/3/1", "/3"), listOf("3-2", "/3/2", "/3")
        ))

        val categories = Category(LogEntry(""), categories).findAllChildCategories("/1")
        assertEquals(2, categories.size)

        val paths = mutableMapOf("/1/1" to true, "/1/2" to true)
        categories.forEach {
            paths.remove(it.category)
        }

        assertEquals(0, paths.size)
    }

    @Test
    @DisplayName("Correctly fetch specific category")
    fun test3() {
        setupCategories(listOf(
            listOf("root", "/", ""),
            listOf("1", "/1", "/"), listOf("2", "/2", "/"), listOf("3", "/3", "/"),
            listOf("1-1", "/1/1", "/1"), listOf("1-2", "/1/2", "/1"),
            listOf("2-1", "/2/1", "/2"), listOf("2-2", "/2/2", "/2"),
            listOf("3-1", "/3/1", "/3"), listOf("3-2", "/3/2", "/3")
        ))

        val _categories = Category(LogEntry(""), categories).findAllChildCategories("/1")
        assertEquals(2, _categories.size)

        val category = Category(LogEntry(""), categories).findOne("/1/1")
        assertNotNull(category)
        assertEquals("/1/1", category.category)
    }

    @Test
    @DisplayName("Correctly fetch all products of a specific category")
    fun test4() {
        setupProducts(listOf(
            listOf("prod1", 100, "usd", listOf("/")),
            listOf("prod2", 200, "usd", listOf("/1")), listOf("prod3", 300, "usd", listOf("/2")), listOf("prod4", 400, "usd", listOf("/3")),
            listOf("prod2-1", 200, "usd", listOf("/1/1")), listOf("prod2-2", 200, "usd", listOf("/1/2")),
            listOf("prod3-1", 300, "usd", listOf("/2/1")), listOf("prod3-2", 200, "usd", listOf("/2/2")),
            listOf("prod4-1", 300, "usd", listOf("/3/1")), listOf("prod4-2", 200, "usd", listOf("/3/2")), listOf("prod4-3", 200, "usd", listOf("/3/3"))
        ))

        val _products = Product(LogEntry(""), products, categories).findByCategory("/")
        assertEquals(1, _products.size)
        assertEquals("/", _products[0].categories[0])
    }

    @Test
    @DisplayName("Correctly fetch all products of a specific categories direct children")
    fun test5() {
        setupCategories(listOf(
            listOf("root", "/", ""),
            listOf("1", "/1", "/"), listOf("2", "/2", "/"), listOf("3", "/3", "/"),
            listOf("1-1", "/1/1", "/1"), listOf("1-2", "/1/2", "/1"),
            listOf("2-1", "/2/1", "/2"), listOf("2-2", "/2/2", "/2"),
            listOf("3-1", "/3/1", "/3"), listOf("3-2", "/3/2", "/3")
        ))

        setupProducts(listOf(
            listOf("prod1", 100, "usd", listOf("/")),
            listOf("prod2", 200, "usd", listOf("/1")), listOf("prod3", 300, "usd", listOf("/2")), listOf("prod4", 400, "usd", listOf("/3")),
            listOf("prod2-1", 200, "usd", listOf("/1/1")), listOf("prod2-2", 200, "usd", listOf("/1/2")),
            listOf("prod3-1", 300, "usd", listOf("/2/1")), listOf("prod3-2", 200, "usd", listOf("/2/2")),
            listOf("prod4-1", 300, "usd", listOf("/3/1")), listOf("prod4-2", 200, "usd", listOf("/3/2")), listOf("prod4-3", 200, "usd", listOf("/3/3"))
        ))

        val _products = Product(LogEntry(""), products, categories).findByDirectCategoryChildren("/")
        assertEquals(3, _products.size)

        val paths = mutableMapOf("/1" to true, "/2" to true, "/3" to true)
        _products.forEach {
            paths.remove(it.categories[0])
        }

        assertEquals(0, paths.size)
    }

    @Test
    @DisplayName("Correctly fetch all products of a specific categories tree")
    fun test6() {
        setupCategories(listOf(
            listOf("root", "/", ""),
            listOf("1", "/1", "/"), listOf("2", "/2", "/"), listOf("3", "/3", "/"),
            listOf("1-1", "/1/1", "/1"), listOf("1-2", "/1/2", "/1"),
            listOf("2-1", "/2/1", "/2"), listOf("2-2", "/2/2", "/2"),
            listOf("3-1", "/3/1", "/3"), listOf("3-2", "/3/2", "/3")
        ))

        setupProducts(listOf(
            listOf("prod1", 100, "usd", listOf("/")),
            listOf("prod2", 200, "usd", listOf("/1")), listOf("prod3", 300, "usd", listOf("/2")), listOf("prod4", 400, "usd", listOf("/3")),
            listOf("prod2-1", 200, "usd", listOf("/1/1")), listOf("prod2-2", 200, "usd", listOf("/1/2")),
            listOf("prod3-1", 300, "usd", listOf("/2/1")), listOf("prod3-2", 200, "usd", listOf("/2/2")),
            listOf("prod4-1", 300, "usd", listOf("/3/1")), listOf("prod4-2", 200, "usd", listOf("/3/2")), listOf("prod4-3", 200, "usd", listOf("/3/3"))
        ))

        val _products = Product(LogEntry(""), products, categories).findByCategoryTree("/1")
        assertEquals(2, _products.size)

        val paths = mutableMapOf("/1/1" to true, "/1/2" to true)
        _products.forEach {
            paths.remove(it.categories[0])
        }

        assertEquals(0, paths.size)
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

    private fun setupCategories(cats: List<List<String>>) {
        cats.forEach {
            Category(LogEntry(""), categories, ObjectId(), it[0], it[1], it[2]).create()
        }
    }

    private fun setupProducts(prods: List<List<Any>>) {
        prods.forEach {
            Product(LogEntry(""), products, categories,
                ObjectId(), it[0] as String, BigDecimal(it[1] as Int),
                it[2] as String, it[3] as List<String>).create()
        }
    }
}
