package com.mtools.schemasimulator.cli

import kotlinx.coroutines.experimental.launch
import org.junit.jupiter.api.Test
import java.io.InputStreamReader
import java.net.URI

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
            URI.create("http://127.0.0.1:14500"),
            InputStreamReader(ClassLoader.getSystemResourceAsStream("SimpleRemoteScenario.kt")).readText()
        ))

        // Setup two slave Executors
        val slaveExecutor1 = SlaveExecutor(SlaveExecutorConfig(
            URI.create("http://127.0.0.1:14500"),
            URI.create("http://127.0.0.1:14501")
        ))

        val slaveExecutor2 = SlaveExecutor(SlaveExecutorConfig(
            URI.create("http://127.0.0.1:14500"),
            URI.create("http://127.0.0.1:14502")
        ))

        val masterThread = Thread(Runnable {
            println("  +++++++++++++++++++++++ START MASTER")
            masterExecutor.start()
            println("  +++++++++++++++++++++++ STOP MASTER")
        })

        val slaveThread1 = Thread(Runnable {
            println("  +++++++++++++++++++++++ START SLAVE1")
            slaveExecutor1.start()
            println("  +++++++++++++++++++++++ STOP SLAVE1")
        })

        val slaveThread2 = Thread(Runnable {
            println("  +++++++++++++++++++++++ START SLAVE2")
            slaveExecutor2.start()
            println("  +++++++++++++++++++++++ STOP SLAVE2")
        })

        println("+++++++++++++++++++++++ START MASTER")
        // Start the threads
        masterThread.start()
        println("+++++++++++++++++++++++ START SLAVE1")
        slaveThread1.start()
        println("+++++++++++++++++++++++ START SLAVE2")
        slaveThread2.start()

        // Wait for them to be done
        println("+++++++++++++++++++++++ WAIT FOR MASTER")
        masterThread.join()
        println("+++++++++++++++++++++++ WAIT FOR SLAVE1")
        slaveThread1.join()
        println("+++++++++++++++++++++++ WAIT FOR SLAVE2")
        slaveThread2.join()
        println("+++++++++++++++++++++++ ALL DONE")
    }
}
