package com.mtools.schemasimulator.schemas.timeseries

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import java.util.*

enum class TimeResolution {
    MINUTE,
    HOUR,
    DAY
}

class TimeSeries(
    logEntry: LogEntry,
    private val timeseries: MongoCollection<Document>,
    val id: Any,
    val series: List<Any>? = null,
    val timestamp: Date? = null,
    val tag: String? = null,
    val resolution: TimeResolution? = null
) : Scenario(logEntry) {

    override fun indexes(): List<Index> = listOf(
        Index(timeseries.namespace.databaseName, timeseries.namespace.collectionName,
            Indexes.compoundIndex(
                Indexes.ascending("tag"),
                Indexes.ascending("timestamp")
            )
        )
    )

    /*
     * Create a new timeseries bucket document on mongodb
     */
    fun create() {
        timeseries.insertOne(Document(mapOf(
            "_id" to id,
            "_tag" to tag,
            "series" to series,
            "timestamp" to timestamp,
            "modifiedOn" to Date()
        )))
    }

    /*
     * Increment a measurement
     */
    fun inc(time: Date, measurement: Double) {
        val updateStatement = Document(mapOf(
            "\$inc" to Document(),
            "\$setOnInsert" to mapOf(
                "tag" to tag,
                "timestamp" to timestamp,
                "resolution" to resolution
            ),
            "\$set" to mapOf(
                "modifiedOn" to Date()
            )
        ))

        // Inc document
        val incDocument = updateStatement["\$inc"] as Document

        // Handle the resolution
        when (resolution) {
            TimeResolution.MINUTE -> {
                incDocument["series.${time.seconds}"]
            }
            TimeResolution.HOUR -> {
                incDocument["series.${time.minutes}.${time.seconds}"]
            }
            TimeResolution.DAY -> {
                incDocument["series.${time.hours}.${time.minutes}.${time.seconds}"]
            }
        }

        // Execute the update
        val result =timeseries.updateOne(Document(mapOf(
            "_id" to id,
            "tag" to tag,
            "timestamp" to timestamp
        )), updateStatement, UpdateOptions().upsert(true))

        if (result.upsertedId == null && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("could not correctly update or upsert the timeseries document with id $id")
        }
    }

    /*
     * Pre allocate an hour worth of measurements in a document
     */
    fun preAllocateMinute(id: Any, tag: String, timestamp: Date) : TimeSeries {
        val series = mutableListOf<Any>()

        for (i in 0 until 60 step 1) {
            series.add(i, 0)
        }

        return TimeSeries(logEntry, timeseries, id, series, timestamp, tag, TimeResolution.MINUTE)
    }

    /*
     * Pre allocate an hour worth of measurements in a document
     */
    fun preAllocateHour(id: Any, tag: String, timestamp: Date) : TimeSeries {
        val series = mutableListOf<Any>()

        for (j in 0 until 60 step 1) {
            val doc = Document()
            series.add(j, doc)

            for (i in 0 until 60 step 1) {
                doc["$i"] = 0
            }
        }

        return TimeSeries(logEntry, timeseries, id, series, timestamp, tag, TimeResolution.MINUTE)
    }

    /*
     * Pre allocate an hour worth of measurements in a document
     */
    fun preAllocateDay(id: Any, tag: String, timestamp: Date) : TimeSeries {
        val series = mutableListOf<Any>()

        for (j in 0 until 60 step 1) {
            val doc = mutableListOf<Any>()
            series.add(j, doc)

            for (i in 0 until 60 step 1) {
                val doc1 = mutableListOf<Any>()
                doc.add(i, doc1)

                for (k in 0 until 60 step 1) {
                    doc1.add(k, 0)
                }
            }
        }

        return TimeSeries(logEntry, timeseries, id, series, timestamp, tag, TimeResolution.MINUTE)
    }
}
