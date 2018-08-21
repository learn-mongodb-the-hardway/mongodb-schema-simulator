package com.mtools.schemasimulator.schemas.shoppingcartreservation

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ReservationShoppingCartTest {
    @Test
    fun successfulAddProductToShoppingCartTest() {
        // Attempt to create a shopping cart
        val action = AddProductToShoppingCart(carts, inventories)
        val product = products.find().first()

        println()

        // Fire the action
        action.execute(mapOf(
            "userId" to 1, "quantity" to 1, "product" to product
        ))

        // Get the generated documents
        val cart = carts
            .find(Document(mapOf("_id" to 1))).first()
        val inventory = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()

        assertEquals(Document(), cart)

        println()

//        val generator = ShoppingCartDataGenerator(ShoppingCartDataGeneratorTest.db)
//        generator.generate(mapOf("numberOfDocuments" to 2))
//
//        assertEquals(2, db.getCollection("products").count())
//        assertEquals(2, db.getCollection("inventories").count())
//        val product = db.getCollection("products").find(Document()).first()
//        assertNotNull(product)
//        val inventory = db.getCollection("inventories").find(Document("_id", product["_id"])).first()
//        assertNotNull(inventory)
//        val shoppingCart = SuccessFullShoppingCart(db)

    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var carts: MongoCollection<Document>
        lateinit var products: MongoCollection<Document>
        lateinit var inventories: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            carts = db.getCollection("carts")
            products = db.getCollection("products")
            inventories = db.getCollection("inventories")

            // Drop collection
            carts.drop()
            products.drop()
            inventories.drop()

            // Generate some test data
            ShoppingCartDataGenerator(db).generate(mapOf(
                "numberOfDocuments" to 1
            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
