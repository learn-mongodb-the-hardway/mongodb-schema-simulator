package com.mtools.schemasimulator.cli

import org.junit.jupiter.api.Test
import java.io.InputStreamReader

class ExecutorTest {

    @Test
    fun correctlyParseLocalConfig() {
        val stream = ClassLoader.getSystemResourceAsStream("SimpleScenario.kt")
        val executor = Executor(InputStreamReader(stream), true)
        executor.execute()
        println()
    }

    @Test
    fun correctlyExecuteMasterSlaveConfig() {
        val stream = ClassLoader.getSystemResourceAsStream("SimpleRemoteScenario.kt")
        val executor = Executor(InputStreamReader(stream), true)
        executor.execute()
        println()
    }

//    val simpleConfig = config {
//        mongodb {
//            url("mongoodb://locahost:27017")
//            db("integration_tests")
//        }
//
//        // Master level coordinator
//        coordinator {
//            // Local running slave thread
//            local {
//                // Load Pattern
//                constant {}
//
//                // Simulation
//                simulation(
//                    SimpleSimulation(seedUserId = 1, numberOfDocuments = 10)
//                )
//            }
//        }
//    }
}
