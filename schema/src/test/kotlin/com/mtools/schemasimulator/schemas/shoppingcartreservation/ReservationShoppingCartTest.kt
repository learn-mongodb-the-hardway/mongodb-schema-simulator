package com.mtools.schemasimulator.schemas.shoppingcartreservation

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

data class f(val value: Any? = Skip(), val klass: Any? = Skip(), val isNull: Boolean = false) {
    class Skip
}

fun Document.shouldContainValues(values: Map<String, f>) {
    values.forEach { path, fieldValue ->
        var field: Any = this

        for(part in path.split(".")) {
            field = when (field) {
                is Document -> field[part]!!
                is ArrayList<*> -> field[part.toInt()]
                is Array<*> -> field[part.toInt()]!!
                else -> field
            }
        }

        // Deal with field value comparision
        if (fieldValue.value != null
            && fieldValue.value::class != f.Skip::class) {
            assertEquals(fieldValue.value, field, "For field $path the expected value ${fieldValue.value} does not match the value $field")
        }

        // Deal with field type comparison
        if (fieldValue.klass != null
            && fieldValue.klass::class != f.Skip::class) {
            assertEquals(fieldValue.klass::class.qualifiedName, field::class.qualifiedName, "For field $path the expected type ${fieldValue.klass::class.qualifiedName} does not match encountered type ${field::class.qualifiedName}")
        }

        // Check nullability of field
        if (!fieldValue.isNull) {
            assertNotNull(field, "field at path was null, expected not null")
        }
    }
}

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

        // Fire the action
        action.execute(mapOf(
            "userId" to userId, "quantity" to 1, "product" to product
        ))

        // Get the generated documents
        val cartR = carts
            .find(Document(mapOf("_id" to userId))).first()
        val inventoryR = inventories
            .find(Document(mapOf("_id" to product["_id"]))).first()

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
