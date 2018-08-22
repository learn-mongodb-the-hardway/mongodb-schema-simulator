package com.mtools.schemasimulator.schemas.shoppingcartreservation

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.schemas.f
import com.mtools.schemasimulator.schemas.g
import com.mtools.schemasimulator.schemas.shouldContainValues
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.collections.ArrayList
import kotlin.test.assertNotNull

class ReservationShoppingCartTest {
    @Test
    fun successfulAddProductToShoppingCartTest() {
        val userId = 1
        // Attempt to create a shopping cart
        val action = AddProductToShoppingCart(carts, inventories)
        val inventory = inventories.find(Document(mapOf(
            "reservations" to mapOf("\$exists" to false), "quantity" to mapOf("\$gte" to 2)
        ))).first()
        val product = products.find(Document(mapOf(
            "_id" to inventory["_id"]
        ))).first()
        assertNotNull(inventory)
        assertNotNull(product)

        // Fire the action
        action.execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(cartR)
        assertNotNull(inventoryR)

        cartR.shouldContainValues(mapOf<String, f>(
            "_id" to f(userId, Integer(0), false),
            "state" to f("active", String(), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to f(f.Skip(), ObjectId(), false),
            "products.0.quantity" to f(1, Integer(0), false),
            "products.0.name" to f(f.Skip(), String(), false),
            "products.0.price" to f(f.Skip(), Double.MIN_VALUE, false)
        ))

        inventoryR.shouldContainValues(mapOf<String, f>(
            "_id" to f(f.Skip(), ObjectId(), false),
            "quantity" to f(inventory.getInteger("quantity") - 1, Integer(0), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "reservations.0._id" to f(userId, Integer(0), false),
            "reservations.0.quantity" to f(1, Integer(0), false),
            "reservations.0.createdOn" to f(f.Skip(), Date(), false)
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

        // Make a reservation first so we can modify it
        AddProductToShoppingCart(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Update the cart
        UpdateReservationQuantityForAProduct(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to 2, "product" to product
        ))

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(cartR)
        assertNotNull(inventoryR)

        cartR.shouldContainValues(mapOf<String, f>(
            "_id" to f(userId, Integer(0), false),
            "state" to f("active", String(), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to f(f.Skip(), ObjectId(), false),
            "products.0.quantity" to f(2, Integer(0), false),
            "products.0.name" to f(f.Skip(), String(), false),
            "products.0.price" to f(f.Skip(), Double.MIN_VALUE, false)
        ))

        inventoryR.shouldContainValues(mapOf<String, f>(
            "_id" to f(f.Skip(), ObjectId(), false),
            "quantity" to f(inventory.getInteger("quantity") - 2, Integer(0), false),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "reservations.0._id" to f(userId, Integer(0), false),
            "reservations.0.quantity" to f(2, Integer(0), false),
            "reservations.0.createdOn" to f(f.Skip(), Date(), false)
        ))
    }

    @Test
    fun failUpdateReservationQuantityForAProductDueToLimitedStockTest() {
        val userId = 3
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
        AddProductToShoppingCart(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Get the generated documents
        val preCartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val preInventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(preCartR)
        assertNotNull(preInventoryR)

        // Update the cart
        UpdateReservationQuantityForAProduct(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to Int.MAX_VALUE, "product" to product
        ))

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(cartR)
        assertNotNull(inventoryR)

        cartR.shouldContainValues(mapOf(
            "_id" to userId,
            "state" to "active",
            "modifiedOn" to f(f.Skip(), Date(), false),
            "products.0._id" to preCartR.g("products.0._id"),
            "products.0.quantity" to preCartR.g("products.0.quantity"),
            "products.0.name" to preCartR.g("products.0.name"),
            "products.0.price" to preCartR.g("products.0.price")
        ))

        inventoryR.shouldContainValues(mapOf(
            "_id" to preInventoryR.g("_id"),
            "quantity" to preInventoryR.g("quantity"),
            "modifiedOn" to f(f.Skip(), Date(), false),
            "reservations.0._id" to preInventoryR.g("reservations.0._id"),
            "reservations.0.quantity" to preInventoryR.g("reservations.0.quantity"),
            "reservations.0.createdOn" to preInventoryR.g("reservations.0.createdOn")
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
        AddProductToShoppingCart(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Force the expire by setting a cutOff date that is expired
        val date = Date(Date().time - 20000000)
        ExpireCarts(carts, inventories).execute(mapOf(
            "cutOffDate" to date
        ))

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
            "modifiedOn" to f(f.Skip(), Date(), false),
            "reservations" to f(f.Skip(), mutableListOf<Document>(), false, 0)
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
        AddProductToShoppingCart(carts, inventories).execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Get the generated documents
        val preCartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val preInventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()
        assertNotNull(preCartR)
        assertNotNull(preInventoryR)

        // Checkout
        CheckoutCart(carts, inventories, orders).execute(mapOf(
            "userId" to userId,
            "name" to "Peter",
            "address" to "Peter street 1",
            "payment" to mapOf(
                "method" to "visa",
                "transaction_id" to "1"
            )
        ))

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

        inventoryR.shouldContainValues(mapOf(
            "_id" to preInventoryR.g("_id"),
            "quantity" to preInventoryR.g("quantity"),
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
            ShoppingCartDataGenerator(db).generate(mapOf(
                "numberOfDocuments" to 5
            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
