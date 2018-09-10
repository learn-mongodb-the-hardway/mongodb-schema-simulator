package com.mtools.schemasimulator.schemas.queue

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.ObjectId
import java.util.*

/*
 * Represents a work item from the queue
 */
class Job(val queue: MongoCollection<Document>, val jobId: Any, val document: Document) {
    /*
     * Sets an end time on the work item signaling it's done
     */
    fun done() {
        val result = queue.updateOne(Document(mapOf(
            "jobId" to jobId
        )), Document(mapOf(
            "\$set" to mapOf(
                "endTime" to Date()
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to set work item with jobId $jobId to done")
        }
    }
}

/*
 * Represents a Queue
 */
class Queue (
    logEntry: LogEntry,
    private val queues: MongoCollection<Document>
) : Scenario(logEntry) {
    // Create a 0 ddate
    val zeroDate = Date(0)

    override fun indexes(): List<Index> = listOf(
        Index(queues.namespace.databaseName, queues.namespace.collectionName,
            Indexes.ascending("startTime")
        ),
        Index(queues.namespace.databaseName, queues.namespace.collectionName,
            Indexes.ascending("createdOn")
        ),
        Index(queues.namespace.databaseName, queues.namespace.collectionName,
            Indexes.descending("priority")
        ),
        Index(queues.namespace.databaseName, queues.namespace.collectionName,
            Indexes.ascending("jobId")
        )
    )

    /*
     * Publish a new item on the queue with a specific priority
     */
    fun publish(priority: Int, payload: Document) {
        // Insert the new item into the queue
        queues.insertOne(Document(mapOf(
            "startTime" to zeroDate,
            "endTime" to zeroDate,
            "jobId" to ObjectId("000000000000000000000000"),
            "createdOn" to Date(),
            "priority" to priority,
            "payload" to payload
        )))
    }

    fun fetchByPriority() : Job {
        // Set sort option
        val sortOrder = Document(mapOf(
            "priority" to -1,
            "createdOn" to 1
        ))

        // Fetch job
        val result = queues.findOneAndUpdate(Document(mapOf(
            "startTime" to zeroDate
        )), Document(mapOf(
            "\$set" to mapOf(
                "startTime" to Date()
            )
        )), FindOneAndUpdateOptions().sort(sortOrder))

        // Check return values
        result ?: throw SchemaSimulatorException("found no message in queue")

        return Job(queues, result["jobId"]!!, result)
    }

    /*
     * Fetch the next item in FIFO fashion (by createdOn timestamp)
     */
    fun fetchFIFO() : Job {
        // Set sort option
        val sortOrder = Document(mapOf(
            "createdOn" to 1
        ))

        // Fetch job
        val result = queues.findOneAndUpdate(Document(mapOf(
            "startTime" to zeroDate
        )), Document(mapOf(
            "\$set" to mapOf(
                "startTime" to Date()
            )
        )), FindOneAndUpdateOptions().sort(sortOrder))

        // Check return values
        result ?: throw SchemaSimulatorException("found no message in queue")

        return Job(queues, result["jobId"]!!, result)
    }
}
