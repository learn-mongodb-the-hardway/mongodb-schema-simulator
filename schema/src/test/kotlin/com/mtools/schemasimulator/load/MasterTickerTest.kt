package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mtools.schemasimulator.SimpleSimulation
import com.mtools.schemasimulator.TestMetricLogger
import com.mtools.schemasimulator.engine.ThreadedSimulationExecutor
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class MasterTickerTest {

    @Test
    fun simpleMasterTickerTest() {
        val logger = TestMetricLogger()

        // Create a Master ticker to execute
        val ticker = MasterTicker(
            slaveTickers = listOf(

                // Local Slave Ticker runs at a constant
                // 2 simulation executions every 100 ms
                LocalSlaveTicker(Constant(
                    executor = ThreadedSimulationExecutor(
                        SimpleSimulation(client, 1, 5), logger
                    ),
                    numberOfCExecutions = 2,
                    executeEveryMilliseconds = 100
                ))
            ),
            // Run for 300 ms (simulated time)
            // we should see 6 simulation executions
            runForNumberOfTicks = 301,
            tickResolutionMiliseconds = 1
        )

        // Start the ticker
        ticker.start()
        // Wait for it to finish
        ticker.join()
        // Execute the stop
        ticker.stop()
    }

    companion object {
        lateinit var client: MongoClient

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
