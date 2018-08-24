package com.mtools.schemasimulator.engine

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.logger.NoopLogger
import com.mtools.schemasimulator.schemas.TestMetricLogger
import com.mtools.schemasimulator.schemas.shoppingcartreservation.AddProductToShoppingCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.CheckoutCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ReservationShoppingCartValues
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGenerator
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGeneratorOptions
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull

class SingleThreadedEngineTest {

    @Test
    fun initializeEngine() {

        val logger = TestMetricLogger()
        val engine = SingleThreadedEngine(logger)
        // Execute a simple simulation
        engine.execute(SimpleSimulation())
        println()



//        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
//        engine.eval("val x = 3")
//        println(engine.eval("x + 2"))  // Prints out 5
    }

    class SimpleSimulation : Simulation(SimulationOptions(iterations = 10)) {
        var userId = 1
        var numberOfDocuments = 5
        lateinit var db: MongoDatabase
        lateinit var products: MongoCollection<Document>
        lateinit var carts: MongoCollection<Document>
        lateinit var inventories: MongoCollection<Document>
        lateinit var orders: MongoCollection<Document>

        override fun mongodbConnection(): MongoClient {
            return client
        }

        override fun beforeAll() {
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

            // Generate some documents
            ShoppingCartDataGenerator(db).generate(ShoppingCartDataGeneratorOptions(
                numberOfDocuments, 100
            ))
        }

        override fun before() {
        }

        override fun run(logEntry: LogEntry) {
            val product = products
                .find()
                .limit(-1)
                .skip(Math.floor(Math.random() * numberOfDocuments).toInt())
                .first()
            assertNotNull(product)

            // Add product to shopping cart
            AddProductToShoppingCart(logEntry, carts, inventories).execute(ReservationShoppingCartValues(
                userId = userId,
                quantity = Math.round(Math.random() * 5).toInt(),
                product = product
            ))

            // Checkout
            CheckoutCart(logEntry, carts, inventories, orders).execute(ReservationShoppingCartValues(
                userId = userId,
                name = "Some random name",
                address = "Aome random address",
                payment = Document(mapOf(
                    "method" to "visa",
                    "transaction_id" to Math.round(Math.random() * Long.MAX_VALUE).toString()
                ))
            ))

            // Update user Id
            userId += 1
        }

        override fun after() {
        }

        override fun afterAll() {
        }
    }

    companion object {
        lateinit var client: MongoClient

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
