package com.mtools.schemasimulator.engine

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.shoppingcartreservation.AddProductToShoppingCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.CheckoutCart
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ReservationShoppingCartValues
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGenerator
import com.mtools.schemasimulator.schemas.shoppingcartreservation.ShoppingCartDataGeneratorOptions
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull

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
