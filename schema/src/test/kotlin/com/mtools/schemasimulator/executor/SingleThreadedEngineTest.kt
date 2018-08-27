package com.mtools.schemasimulator.executor

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class SingleThreadedEngineTest {

    @Test
    fun initializeEngine() {

//        val logger = TestMetricLogger()
//        val engine = ThreadedSimulationExecutor(logger)
//        // Execute a simple simulation
//        engine.execute(listOf(
//            SimpleSimulation()
//        ))
//
//        println()



//        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
//        engine.eval("val x = 3")
//        println(engine.eval("x + 2"))  // Prints out 5
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
