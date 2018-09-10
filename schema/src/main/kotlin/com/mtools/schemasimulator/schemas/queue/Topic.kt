package com.mtools.schemasimulator.schemas.queue

import com.mongodb.CursorType
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import org.bson.Document
import java.util.*

enum class TopicState {
    UNKNOWN,
    INITIALIZED,
    LISTENING,
    CLOSED
}

class Topic (
    logEntry: LogEntry,
    private val db: MongoDatabase,
    private val topics: MongoCollection<Document>,
    private val sizeInBytes: Long,
    private val maxDocuments: Long
) : Scenario(logEntry) {
    var cursor: MongoCursor<Document>? = null

    override fun indexes(): List<Index> {
        return listOf()
    }

    /*
     * Create a topic
     */
    fun create() = log("create") {
        db.createCollection(
            topics.namespace.collectionName,
            CreateCollectionOptions()
                .capped(true)
                .sizeInBytes(sizeInBytes)
                .maxDocuments(maxDocuments)
        )
    }

    /*
     * Push an object to the topic
     */
    fun publish(document: Document) = log("publish") {
        topics.insertOne(Document(mapOf(
            "createdOn" to Date(),
            "payload" to document
        )))
    }

    /*
     * Simple cursor builder, does not try to deal with reconnect etc
     */
    fun listen(awaitData: Boolean = true, from: Date?) = log("listen") {
        val query = Document()

        if (from != null) {
            query["createdOn"] = Document(mapOf(
                "\$gte" to from
            ))
        }

        // Set up cursor
        val cursorType = if (awaitData) CursorType.TailableAwait else CursorType.Tailable
        cursor = topics.find(query).cursorType(cursorType).iterator()
    }

    /*
     * Read the next topic entry
     */
    fun next() : Document? {
        return cursor?.tryNext()
    }

    /*
     * Close the cursor
     */
    fun close() {
        cursor?.close()
    }
}
