package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File
import java.io.InputStreamReader
import java.net.URI
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ExecutorTest {

    @Test
    @Disabled
    fun correctlyParseLocalConfig() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/SimpleScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/SimpleScenario.png"),
            graphOutputDPI = 300
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    @Disabled
    fun executeTransaction() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/TransactionScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/TransactionScenario.png"),
            graphOutputDPI = 300,
            configurationMethod = "configureTransactions",
            graphFilters = listOf("total")
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    @Disabled
    fun executeMongoTransaction() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/MongoTransactionScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenario.png"),
            graphOutputDPI = 300,
            configurationMethod = "configureMongoTransactions",
            graphFilters = listOf("total")
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    @Disabled
    fun correctlyExecuteMasterWorkerConfig() {
        // Setup master
        val masterExecutor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("remote/SimpleRemoteScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/SimpleRemoteScenario.png"),
            graphOutputDPI = 300
        ))

        // Setup two worker Executors
        val workerExecutor1 = WorkerExecutor(WorkerExecutorConfig(
            URI.create("http://127.0.0.1:14500"),
            URI.create("http://127.0.0.1:14501")
        ))

        val workerExecutor2 = WorkerExecutor(WorkerExecutorConfig(
            URI.create("http://127.0.0.1:14500"),
            URI.create("http://127.0.0.1:14502")
        ))

        val masterThread = Thread(Runnable {
//            println("  +++++++++++++++++++++++ START MASTER")
            masterExecutor.start()
//            println("  +++++++++++++++++++++++ STOP MASTER")
        })

        val workerThread1 = Thread(Runnable {
//            println("  +++++++++++++++++++++++ START WORKER1")
            workerExecutor1.start()
//            println("  +++++++++++++++++++++++ STOP WORKER1")
        })

        val workerThread2 = Thread(Runnable {
//            println("  +++++++++++++++++++++++ START WORKER2")
            workerExecutor2.start()
//            println("  +++++++++++++++++++++++ STOP WORKER2")
        })

//        println("+++++++++++++++++++++++ START MASTER")
        // Start the threads
        masterThread.start()
//        println("+++++++++++++++++++++++ START WORKER1")
        workerThread1.start()
//        println("+++++++++++++++++++++++ START WORKER2")
        workerThread2.start()

        // Wait for them to be done
//        println("+++++++++++++++++++++++ WAIT FOR MASTER")
        masterThread.join()
//        println("+++++++++++++++++++++++ WAIT FOR WORKER1")
        workerThread1.join()
//        println("+++++++++++++++++++++++ WAIT FOR WORKER2")
        workerThread2.join()
//        println("+++++++++++++++++++++++ ALL DONE")

        // Let's do some basic checking of the available information
        val db = MongoClient(MongoClientURI("mongodb://127.0.0.1")).getDatabase("integration_tests")
        var collection = db.getCollection("carts")
        assertNotNull(collection)
        assertTrue(collection.count() > 0)
        assertEquals(3, collection.listIndexes().count())

        collection = db.getCollection("inventories")
        assertNotNull(collection)
        assertTrue(collection.count() > 0)
        assertEquals(2, collection.listIndexes().count())

        collection = db.getCollection("orders")
        assertNotNull(collection)
        assertTrue(collection.count() > 0)
        assertEquals(1, collection.listIndexes().count())

        collection = db.getCollection("products")
        assertNotNull(collection)
        assertTrue(collection.count() > 0)
        assertEquals(1, collection.listIndexes().count())
    }
}
