package com.mtools.schemasimulator.schemas.shoppingcart

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class ShoppingCartTest {
    @Test
    fun simpleGenerationTest() {
//        val generator = ShoppingCartDataGenerator(ShoppingCartDataGeneratorTest.db)
//        generator.generate(mapOf("numberOfDocuments" to 2))
//
//        assertEquals(2, db.getCollection("products").count())
//        assertEquals(2, db.getCollection("inventories").count())
//        val product = db.getCollection("products").find(Document()).first()
//        assertNotNull(product)
//        val inventory = db.getCollection("inventories").find(Document("_id", product["_id"])).first()
//        assertNotNull(inventory)
        val shoppingCart = SuccessFullShoppingCart(db)

    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            db.getCollection("products").drop()
            db.getCollection("inventories").drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
