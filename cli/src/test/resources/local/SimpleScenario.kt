package local

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.executor.Simulation
import com.mtools.schemasimulator.executor.SimulationOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGenerator
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGeneratorOptions
import org.bson.Document
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertNotNull
import com.mtools.schemasimulator.cli.config.config
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCart

class ReservationCartSimulation(seedUserId: Int = 1,
                       private val numberOfDocuments: Int = 5) : Simulation(SimulationOptions(iterations = 10)) {
    override fun init(client: MongoClient) {
        this.client = client
        // Drop the database
        client.getDatabase("integration_tests").drop()

        // Get the collections
        db = client.getDatabase("integration_tests")
        val carts = db.getCollection("carts")
        val inventories = db.getCollection("inventories")
        val orders = db.getCollection("orders")

        // Generate some documents
        ShoppingCartDataGenerator(db).generate(ShoppingCartDataGeneratorOptions(
            numberOfDocuments, 100
        ))

        createIndexes(ShoppingCart(LogEntry("", 0), carts, inventories, orders))
    }

    lateinit var client: MongoClient
    lateinit var db: MongoDatabase
    lateinit var products: MongoCollection<Document>
    lateinit var carts: MongoCollection<Document>
    lateinit var inventories: MongoCollection<Document>
    lateinit var orders: MongoCollection<Document>
    private var userId: AtomicInteger = AtomicInteger(seedUserId)

    override fun mongodbConnection(): MongoClient {
        return client
    }

    override fun beforeAll(client: MongoClient) {
        this.client = client
        db = client.getDatabase("integration_tests")
        carts = db.getCollection("carts")
        products = db.getCollection("products")
        inventories = db.getCollection("inventories")
        orders = db.getCollection("orders")
    }

    override fun before(client: MongoClient) {
    }

    override fun run(logEntry: LogEntry) {
        val product = products
            .find()
            .limit(-1)
            .skip(Math.floor(Math.random() * numberOfDocuments).toInt())
            .first()
        assertNotNull(product)

        // Get current userId
        val currentUserId = userId.incrementAndGet()
        val cart = ShoppingCart(logEntry, carts, inventories, orders)

        // Add product to shopping cart
        cart.addProduct(
            userId = currentUserId,
            quantity = Math.round(Math.random() * 5).toInt(),
            product = product
        )

        // Checkout
        cart.checkout(
            userId = currentUserId,
            name = "Some random name",
            address = "Acme random address",
            payment = Document(mapOf(
                "method" to "visa",
                "transaction_id" to Math.round(Math.random() * Long.MAX_VALUE).toString()
            ))
        )
    }

    override fun after(client: MongoClient) {
    }

    override fun afterAll(client: MongoClient) {
    }
}

fun configure() : Config {
    val tickResolution = 1L
//val numberOfTicks = 300L
val numberOfTicks = 3000L
//    val numberOfTicks = 30000L
//val numberOfTicks = 30000L * 2 * 3
    val numberOfDocuments = 10

    return config {
        mongodb {
            url("mongodb://127.0.0.1:27017/?connectTimeoutMS=1000")
            db("integration_tests")
        }

        // Master level coordinator
        coordinator {
            // Each Master tick is every 1 millisecond
            tickResolutionMilliseconds(tickResolution)
            // Run for 1000 ticks or in this case 1000 simulated milliseconds
            runForNumberOfTicks(numberOfTicks)

            // Local running worker thread
            local {
                name("local1")

                // Constant Load Pattern
                constant {
                    // Each tick produces two concurrently
                    // executed instances of the simulation
                    numberOfCExecutions(10)
                    // Execute every 100 milliseconds
                    executeEveryMilliseconds(2)
                }

                // Simulation
                simulation(
                    ReservationCartSimulation(seedUserId = 1, numberOfDocuments = numberOfDocuments)
                )
            }

            // Local running worker thread
            local {
                name("local2")

                // Constant Load Pattern
                constant {
                    // Each tick produces two concurrently
                    // executed instances of the simulation
                    numberOfCExecutions(10)
                    // Execute every 100 milliseconds
                    executeEveryMilliseconds(4)
                }

                // Simulation
                simulation(
                    ReservationCartSimulation(seedUserId = 10000, numberOfDocuments = numberOfDocuments)
                )
            }
        }
    }
}
