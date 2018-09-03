package com.mtools.schemasimulator.cli

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
    fun correctlyExecuteMasterWorkerConfig() {
        // Setup master
        val masterExecutor = MasterExecutor(MasterExecutorConfig(
            true,
            URI.create("http://127.0.0.1:14500"),
            InputStreamReader(ClassLoader.getSystemResourceAsStream("SimpleRemoteScenario.kt")).readText()
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
            println("  +++++++++++++++++++++++ START MASTER")
            masterExecutor.start()
            println("  +++++++++++++++++++++++ STOP MASTER")
        })

        val workerThread1 = Thread(Runnable {
            println("  +++++++++++++++++++++++ START WORKER1")
            workerExecutor1.start()
            println("  +++++++++++++++++++++++ STOP WORKER1")
        })

        val workerThread2 = Thread(Runnable {
            println("  +++++++++++++++++++++++ START WORKER2")
            workerExecutor2.start()
            println("  +++++++++++++++++++++++ STOP WORKER2")
        })

        println("+++++++++++++++++++++++ START MASTER")
        // Start the threads
        masterThread.start()
        println("+++++++++++++++++++++++ START WORKER1")
        workerThread1.start()
        println("+++++++++++++++++++++++ START WORKER2")
        workerThread2.start()

        // Wait for them to be done
        println("+++++++++++++++++++++++ WAIT FOR MASTER")
        masterThread.join()
        println("+++++++++++++++++++++++ WAIT FOR WORKER1")
        workerThread1.join()
        println("+++++++++++++++++++++++ WAIT FOR WORKER2")
        workerThread2.join()
        println("+++++++++++++++++++++++ ALL DONE")
    }
}
