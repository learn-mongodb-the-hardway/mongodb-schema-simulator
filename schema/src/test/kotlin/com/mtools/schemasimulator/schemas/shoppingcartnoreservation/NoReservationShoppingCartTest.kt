package com.mtools.schemasimulator.schemas.shoppingcartnoreservation

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.createLogEntry
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.f
import com.mtools.schemasimulator.g
import com.mtools.schemasimulator.shouldContainValues
import com.mtools.schemasimulator.shouldNoContainFields
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertNotNull

@Suppress("UNCHECKED_CAST")
class NoReservationShoppingCartTest {
    @Test
    fun successfulAddProductToShoppingCartTest() {
        val userId = 1
        val cart = ShoppingCart(createLogEntry(), carts, inventories, orders)
        // Attempt to create a shopping cart
        val inventory = inventories.find(Document(mapOf(
            "reservations" to mapOf("\$exists" to false), "quantity" to mapOf("\$gte" to 2)
        ))).first()
        val product = products.find(Document(mapOf(
            "_id" to inventory["_id"]
        ))).first()
        assertNotNull(inventory)
        assertNotNull(product)

        // Fire the action
        cart.addProduct(userId, 1, product)

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        assertNotNull(cartR)

        cartR.shouldContainValues(mapOf<String, f>(
            "_id" to f(userId, Integer(0), false),
            "state" to f("active", String(), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to f(f.Skip(), ObjectId(), false),
            "products.0.quantity" to f(1, Integer(0), false),
            "products.0.name" to f(f.Skip(), String(), false),
            "products.0.price" to f(f.Skip(), Double.MIN_VALUE, false)
        ))
    }

    @Test
    fun successfulUpdateReservationQuantityForAProductTest() {
        val userId = 2
        // Attempt to create a shopping cart
        val inventory = inventories.find(Document(mapOf(
            "reservations" to mapOf("\$exists" to false), "quantity" to mapOf("\$gte" to 3)
        ))).first()
        val product = products.find(Document(mapOf(
            "_id" to inventory["_id"]
        ))).first()
        assertNotNull(inventory)
        assertNotNull(product)

        val cart = ShoppingCart(createLogEntry(), carts, inventories, orders)
        cart.addProduct(userId, 1, product)
        cart.updateProduct(userId, 2, product)

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        assertNotNull(cartR)

        cartR.shouldContainValues(mapOf<String, f>(
            "_id" to f(userId, Integer(0), false),
            "state" to f("active", String(), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to f(f.Skip(), ObjectId(), false),
            "products.0.quantity" to f(2, Integer(0), false),
            "products.0.name" to f(f.Skip(), String(), false),
            "products.0.price" to f(f.Skip(), Double.MIN_VALUE, false)
        ))
    }

    @Test
    fun expireCarts() {
        val userId = 4
        // Attempt to create a shopping cart
        val inventory = inventories.find(Document(mapOf(
            "reservations" to mapOf("\$exists" to false), "quantity" to mapOf("\$gte" to 1)
        ))).first()
        val product = products.find(Document(mapOf(
            "_id" to inventory["_id"]
        ))).first()
        assertNotNull(inventory)
        assertNotNull(product)

        // Make a reservation first so we can modify it
        val cart = ShoppingCart(createLogEntry(), carts, inventories, orders)
        cart.addProduct(userId, 1, product)

        // Force the expireAllCarts by setting a cutOff date that is expired
        val date = Date(Date().time + 20000000)
        cart.expireAllCarts(date)

        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(cartR)
        assertNotNull(inventoryR)

        cartR.shouldContainValues(mapOf(
            "_id" to userId,
            "state" to "expired",
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products" to f(f.Skip(), mutableListOf<Document>(), false, 1),
            "products.0._id" to f(f.Skip(), ObjectId(), false),
            "products.0.quantity" to f(1, Integer(0), false),
            "products.0.name" to f(f.Skip(), String(), false),
            "products.0.price" to f(f.Skip(), Double.MIN_VALUE, false)
        ))

        inventoryR.shouldContainValues(mapOf(
            "_id" to f(f.Skip(), ObjectId(), false),
            "quantity" to inventory.g("quantity"),
            "modifiedOn" to f(f.Skip(), Date(), false)
        ))

        inventoryR.shouldNoContainFields(listOf(
            "reservations"
        ))
    }

    @Test
    fun checkoutCart() {
        val userId = 5
        // Attempt to create a shopping cart
        val inventory = inventories.find(Document(mapOf(
            "reservations" to mapOf("\$exists" to false), "quantity" to mapOf("\$gte" to 1)
        ))).first()
        val product = products.find(Document(mapOf(
            "_id" to inventory["_id"]
        ))).first()
        assertNotNull(inventory)
        assertNotNull(product)

        // Make a reservation first so we can modify it
        val cart = ShoppingCart(createLogEntry(), carts, inventories, orders)
        cart.addProduct(userId, 1, product)

        // Get the generated documents
        val preCartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val preInventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(preCartR)
        assertNotNull(preInventoryR)

        // Checkout
        cart.checkout(
            userId = userId,
            name = "Peter",
            address = "Peter street 1",
            payment = Document(mapOf(
                "method" to "visa",
                "transaction_id" to "1"
            ))
        )

        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        val orderR = orders
            .find(Document(mapOf("userId" to userId))).first()
        assertNotNull(cartR)
        assertNotNull(inventoryR)
        assertNotNull(orderR)

        cartR.shouldContainValues(mapOf(
            "_id" to userId,
            "state" to "complete",
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to preCartR.g("products.0._id"),
            "products.0.quantity" to preCartR.g("products.0.quantity"),
            "products.0.name" to preCartR.g("products.0.name"),
            "products.0.price" to preCartR.g("products.0.price")
        ))

        val quantity = preInventoryR.g("quantity") as Int

        inventoryR.shouldContainValues(mapOf(
            "_id" to preInventoryR.g("_id"),
            "quantity" to quantity - 1,
            "modifiedOn" to f(f.Skip(), Date(), false),
            "reservations" to f(f.Skip(), mutableListOf<Document>(), false, 0)
        ))

        orderR.shouldContainValues(mapOf(
            "_id" to f(f.Skip(), ObjectId(), false),
            "userId" to userId,
            "createdOn" to f(f.Skip(), Date(), false),
            "shipping.name" to "Peter",
            "shipping.address" to "Peter street 1",
            "payment.method" to "visa",
            "payment.transaction_id" to "1",
            "products.0._id" to preCartR.g("products.0._id"),
            "products.0.quantity" to preCartR.g("products.0.quantity"),
            "products.0.name" to preCartR.g("products.0.name"),
            "products.0.price" to preCartR.g("products.0.price")
        ))
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var carts: MongoCollection<Document>
        lateinit var products: MongoCollection<Document>
        lateinit var inventories: MongoCollection<Document>
        lateinit var orders: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            carts = db.getCollection("carts")
            products = db.getCollection("products")
            inventories = db.getCollection("inventories")
            orders = db.getCollection("orders")

            // Drop collection
            carts.drop()
            products.drop()
            inventories.drop()
            orders.drop()

            // Generate some test data
            ShoppingCartDataGenerator(db).generate(ShoppingCartDataGeneratorOptions(
                5, 100
            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
