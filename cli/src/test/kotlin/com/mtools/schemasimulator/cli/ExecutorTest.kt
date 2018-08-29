package com.mtools.schemasimulator.cli

import kotlinx.coroutines.experimental.launch
import org.junit.jupiter.api.Test
import java.io.InputStreamReader

class ExecutorTest {

    @Test
    fun correctlyParseLocalConfig() {
//        val stream = ClassLoader.getSystemResourceAsStream("SimpleScenario.kt")
//        val executor = MasterExecutor(InputStreamReader(stream))
//        executor.start()
//        println()
    }

    @Test
    fun correctlyExecuteMasterSlaveConfig() {
        // Setup master
        val masterExecutor = MasterExecutor(MasterExecutorConfig(
            true,
            InputStreamReader(ClassLoader.getSystemResourceAsStream("SimpleRemoteScenario.kt")).readText(),
            "127.0.0.1", 14500
        ))

        // Setup two slave Executors
        val slaveExecutor1 = SlaveExecutor(SlaveExecutorConfig(
            "127.0.0.1", 14500,
            "127.0.0.1", 14501
        ))

        val slaveExecutor2 = SlaveExecutor(SlaveExecutorConfig(
            "127.0.0.1", 14500,
            "127.0.0.1", 14502
        ))

//        // Start master
//        val masterJob = launch {
//            masterExecutor.start()
//        }
//
//        // Start slaves
//        val slave1Job = launch {
//            slaveExecutor1.start()
//        }

        Thread(Runnable {
            slaveExecutor1.start()
        }).start()

        Thread(Runnable {
            masterExecutor.start()
        }).start()

//        slaveExecutor2.start()
            while(true) {
                Thread.sleep(1000)
            }

//        val stream = ClassLoader.getSystemResourceAsStream("SimpleRemoteScenario.kt")
//        val executor = MasterExecutor(InputStreamReader(stream))
//        executor.start()
//        println()
//        }
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
