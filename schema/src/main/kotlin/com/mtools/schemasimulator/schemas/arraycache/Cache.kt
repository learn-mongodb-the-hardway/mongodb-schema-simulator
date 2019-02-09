package com.mtools.schemasimulator.schemas.arraycache

import com.mongodb.client.MongoCollection
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.ObjectId

class Cache(
    logEntry: LogEntry,
    private val cache: MongoCollection<Document>,
    private val sliceAt: Int = 0,
    val id: ObjectId = ObjectId()) : Scenario(logEntry) {

    fun create(cacheDocument: Document? = null) = log("create") {
        val data = mutableListOf<Document>()

        // Pre-allocate cache
        if (cacheDocument != null) {
            for (i in 0 until sliceAt) {
                data += cacheDocument
            }
        }

        // Insert the metadata
        cache.insertOne(Document(mapOf(
            "_id" to id,
            "sliceAt" to sliceAt,
            "data" to data
        )))
    }

    fun push(items: List<Document> = listOf(), position: Int? = null) = log("push") {
        // Create push operation
        val pushOperation = Document(mapOf(
            "data" to mutableMapOf(
                "\$each" to items,
                "\$slice" to sliceAt.unaryMinus()
            )
        ))

        // We provided a position for adding the items in the cache
        if (position != null && pushOperation["data"] is MutableMap<*, *>) {
            @Suppress("UNCHECKED_CAST")
            (pushOperation["data"] as MutableMap<String, Any>)["\$position"] = position
        }

        val result = cache.updateOne(
            Document(mapOf(
                "_id" to id
            )),
            Document(mapOf(
                "\$push" to pushOperation
            )))

        if (result.isModifiedCountAvailable && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to push items to cache object with id $id")
        }
    }

    override fun indexes() : List<Index> = listOf()
}
