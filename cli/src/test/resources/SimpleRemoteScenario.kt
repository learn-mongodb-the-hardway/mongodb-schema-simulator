import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.executor.Simulation
import com.mtools.schemasimulator.executor.SimulationOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.shoppingcartreservation.AddProductToShoppingCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.CheckoutCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ReservationShoppingCartValues
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGenerator
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGeneratorOptions
import org.bson.Document
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertNotNull
import com.mtools.schemasimulator.cli.config.config

class SimpleSimulation(seedUserId: Int = 1,
                       private val numberOfDocuments: Int = 5) : Simulation(SimulationOptions(iterations = 10)) {
    override fun init(client: MongoClient) {
        this.client = client
    }

    private var userId: AtomicInteger = AtomicInteger(seedUserId)

    lateinit var client: MongoClient
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

        // Get current userId
        val currentUserId = userId.incrementAndGet()

        // Add product to shopping cart
        AddProductToShoppingCart(logEntry, carts, inventories).execute(ReservationShoppingCartValues(
            userId = currentUserId,
            quantity = Math.round(Math.random() * 5).toInt(),
            product = product
        ))

        // Checkout
        CheckoutCart(logEntry, carts, inventories, orders).execute(ReservationShoppingCartValues(
            userId = currentUserId,
            name = "Some random name",
            address = "Aome random address",
            payment = Document(mapOf(
                "method" to "visa",
                "transaction_id" to Math.round(Math.random() * Long.MAX_VALUE).toString()
            ))
        ))
    }

    override fun after() {
    }

    override fun afterAll() {
    }
}

config {
    mongodb {
        url("mongodb://127.0.0.1:27017/?connectTimeoutMS=1000")
        db("integration_tests")
    }

    // Master level coordinator
    coordinator {
        // Each Master tick is every 1 millisecond
        tickResolutionMilliseconds(1)
        // Run for 1000 ticks or in this case 1000 simulated milliseconds
        runForNumberOfTicks(1000)

        // Local running slave thread
        remote {
            name("local1")

            // Constant Load Pattern
            constant {
                // Each tick produces two concurrently
                // executed instances of the simulation
                numberOfCExecutions(2)
                // Execute every 100 milliseconds
                executeEveryMilliseconds(100)
            }

            // Simulation
            simulation(
                SimpleSimulation(seedUserId = 1, numberOfDocuments = 10)
            )
        }
    }
}
