package com.mtools.schemasimulator.schemas.timeseries

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TimeSeriesTest {

    @Test
    @DisplayName("Correctly create and execute ten increments on a timeseries object")
    fun test1() {
        val timestamp = DateTime(2018, 10, 1, 1, 0, 0).toDate()
        // Create a new timeseries instance
        val timeSeries = TimeSeries(
            LogEntry(""), timeseries, ObjectId(),
            mutableListOf(), timestamp, "device1",
            TimeResolution.MINUTE)
        timeSeries.create()

        // Increment the counter
        for (i in 0 until 60 step 1) {
            val date = DateTime(2018, 10, 1, 1, 0, i).toDate()
            timeSeries.inc(date, 1.0)
        }

        // Grab the document
        val doc = timeseries.find(Document(mapOf(
            "_id" to timeSeries.id
        ))).firstOrNull()
        assertNotNull(doc)

        val series = doc?.get("series") as List<Double>
        for (n in series) {
            assertEquals(1.0, n)
        }
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var timeseries: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            timeseries = db.getCollection("timeseries")

            // Drop collection
            timeseries.drop()

//            // Generate some test data
//            ShoppingCartDataGenerator(db).generate(ShoppingCartDataGeneratorOptions(
//                5, 100
//            ))
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
