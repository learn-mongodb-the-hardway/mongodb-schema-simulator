package com.mtools.schemasimulator.cli

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File
import java.io.InputStreamReader
import java.net.URI

@Disabled
class AccountTransactionSimulationTest {
    @Test
    fun executeMongoTransactionLocal() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioLocal.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioLocal.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    fun executeMongoTransactionLocalMajority() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioLocalMajority.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioLocalMajority.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    fun executeMongoTransactionMajority() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioMajority.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioMajority.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    fun executeMongoTransactionMajorityMajority() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioMajorityMajority.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioMajorityMajority.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    fun executeMongoTransactionSnapshot() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioSnapshot.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioSnapshot.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }

    @Test
    fun executeMongoTransactionSnapshotMajority() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = true,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_transaction/MongoTransactionScenarioSnapshotMajority.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/MongoTransactionScenarioSnapshotMajority.png"),
            graphOutputDPI = 300,
            configurationMethod = "configure",
            graphFilters = listOf("total"),
            skipTicks = 5000
        ))

        val masterThread = Thread(Runnable {
            executor.start()
        })

        masterThread.start()
        masterThread.join()
    }
}
