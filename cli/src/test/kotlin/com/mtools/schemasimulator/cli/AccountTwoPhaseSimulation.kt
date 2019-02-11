package com.mtools.schemasimulator.cli

import org.junit.jupiter.api.Test
import java.io.File
import java.io.InputStreamReader
import java.net.URI

class AccountTwoPhaseSimulation {

    @Test
    fun executeW1() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = false,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_twophase/TransactionScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/TransactionScenario.png"),
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
    fun executeMajority() {
        // Setup master
        val executor = MasterExecutor(MasterExecutorConfig(
            master = false,
            uri = URI.create("http://127.0.0.1:14500"),
            config = InputStreamReader(ClassLoader.getSystemResourceAsStream("local/account_twophase/TransactionMajorityScenario.kt")).readText(),
            graphOutputFilePath = File("${System.getProperty("user.dir")}/TransactionMajorityScenario.png"),
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
