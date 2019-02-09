package local1

import com.mongodb.MongoClient
import com.mongodb.ReadConcern
import com.mongodb.TransactionOptions
import com.mongodb.WriteConcern
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.config
import com.mtools.schemasimulator.executor.Simulation
import com.mtools.schemasimulator.executor.SimulationOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.accounttransaction.Account
import com.mtools.schemasimulator.schemas.accounttransaction.Transaction
import com.mtools.schemasimulator.schemas.shoppingcartreservation.AccountDataGenerator
import com.mtools.schemasimulator.schemas.shoppingcartreservation.AccountDataGeneratorOptions
import org.bson.Document
import java.math.BigDecimal

class MongoTransactionSimulation(private val numberOfAccounts: Int = 100) : Simulation(SimulationOptions(iterations = 10)) {
    override fun init(client: MongoClient) {
        this.client = client
        // Drop the database
        client.getDatabase("integration_tests").drop()

        // Get the collections
        db = client.getDatabase("integration_tests")

        // Create the collections
        db.createCollection("accounts")
        db.createCollection("transactions")

        // Get collections
        val accounts = db.getCollection("accounts")
        val transactions = db.getCollection("transactions")

        // Generate some accounts
        AccountDataGenerator(db).generate(AccountDataGeneratorOptions(
            numberOfAccounts = numberOfAccounts
        ))

        createIndexes(Account(LogEntry("", 0), client, accounts, transactions, "Joe", BigDecimal(0)))
        createIndexes(Transaction(LogEntry("", 0), transactions,
            Account(LogEntry("", 0), client, accounts, transactions, "Joe", BigDecimal(0)),
            Account(LogEntry("", 0), client, accounts, transactions, "Joe", BigDecimal(0)),
            BigDecimal(100)))
    }

    private lateinit var client: MongoClient
    private lateinit var db: MongoDatabase
    private lateinit var accounts: MongoCollection<Document>
    private lateinit var transactions: MongoCollection<Document>
    private var accountNames: MutableList<String> = mutableListOf()
    private var index1 = 0
    private var index2 = 1

    override fun mongodbConnection(): MongoClient {
        return client
    }

    override fun beforeAll(client: MongoClient) {
        this.client = client
        db = client.getDatabase("integration_tests")
        accounts = db.getCollection("accounts")
        transactions = db.getCollection("transactions")

        // Read all the account names so we can use them to perform transactions
        accountNames = accounts.find().map { it.getString("name") }.toMutableList()
    }

    override fun before(client: MongoClient) {
    }

    override fun run(logEntry: LogEntry) {
        if (index1 > accountNames.size) index1 = 0
        if (index2 > accountNames.size) index2 = 0

        // Locate two
        val name1 = accountNames[index1++ % accountNames.size]
        var name2 = accountNames[index2++ % accountNames.size]

        if (name1 == name2) {
            name2 = accountNames[index2++ % accountNames.size]
        }

        // Retrieve the accounts
        val account1 = Account(logEntry, client, accounts, transactions, name1, BigDecimal(0))
        val account2 = Account(logEntry, client, accounts, transactions, name2, BigDecimal(0))

        // Refresh the accounts
        account1.reload()
        account2.reload()

        // Perform a transfer of 1
        account1.transfer(account2, BigDecimal(1), TransactionOptions.builder()
            .readConcern(ReadConcern.MAJORITY)
            .writeConcern(WriteConcern.W1)
            .build())
    }

    override fun after(client: MongoClient) {
    }

    override fun afterAll(client: MongoClient) {
    }
}

fun configure() : Config {
    val tickResolution = 1L
//    val numberOfTicks = 300L
//    val numberOfTicks = 3000L
    val numberOfTicks = 35000L
//    val numberOfTicks = 30000L * 2 * 3
    val numberOfDocuments = 1500

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
                    numberOfCExecutions(2)
                    // Execute every 100 milliseconds
                    executeEveryMilliseconds(2)
                }

                // Simulation
                simulation(
                    MongoTransactionSimulation(numberOfAccounts = numberOfDocuments)
                )
            }

            // Local running worker thread
            local {
                name("local2")

                // Constant Load Pattern
                constant {
                    // Each tick produces two concurrently
                    // executed instances of the simulation
                    numberOfCExecutions(2)
                    // Execute every 100 milliseconds
                    executeEveryMilliseconds(4)
                }

                // Simulation
                simulation(
                    MongoTransactionSimulation(numberOfAccounts = numberOfDocuments)
                )
            }
        }
    }
}
