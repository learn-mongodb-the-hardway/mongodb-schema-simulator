package com.mtools.schemasimulator.schemas.arraycache

import com.mongodb.client.MongoCollection
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.ObjectId

class Cache(
    private val cache: MongoCollection<Document>,
    private val sliceAt: Int = 0,
    private val id: ObjectId = ObjectId()) {

    fun create(cacheDocument: Document?) {
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

    fun push(items: List<Document> = listOf(), position: Int? = null) {
        // Create push operation
        val pushOperation = Document(mapOf(
            "data" to mutableMapOf(
                "\$each" to items,
                "\$slice" to sliceAt.unaryMinus()
            )
        ))

        // We provided a position for adding the items in the cache
        if (position != null) {
            val data = pushOperation["data"] as MutableMap<String, Any>
            data["\$position"] = position
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

    fun indexes() : List<Index> = listOf()
}

//data class CacheValues(
//    val cacheDocument: Document?,
//    val sliceAt: Int = 0,
//    val items: List<Document>,
//    val position: Int?,
//    val cacheId: ObjectId?
//): ActionValues
//
//
//class CacheIndexes(private val cache: MongoCollection<Document>) : IndexClass {
//    override val indexes = listOf(
//        Index(cache.namespace.databaseName, cache.namespace.collectionName,
//            Indexes.ascending("reservations._id")
//        )
//    )
//}

//class CreateCache(logEntry: LogEntry, private val cache: MongoCollection<Document>) : Action(logEntry) {
//    override fun run(values: ActionValues): Map<String, Any> {
//        if (!(values is CacheValues)) {
//            throw SchemaSimulatorException("values passed to action must be of type CacheIndexes")
//        }
//
//        // Array
//        val data = mutableListOf<Document>()
//        val id = ObjectId()
//
//        // Pre-allocate cache
//        if (values.cacheDocument != null) {
//            for (i in 0 until values.sliceAt) {
//                data += values.cacheDocument
//            }
//        }
//
//        // Insert the metadata
//        cache.insertOne(Document(mapOf(
//            "_id" to id,
//            "sliceAt" to values.sliceAt,
//            "data" to data
//        )))
//
//        return mapOf("_id" to id)
//    }
//}
//
//class PushToCache(logEntry: LogEntry, private val cache: MongoCollection<Document>) : Action(logEntry) {
//    override fun run(values: ActionValues): Map<String, Any> {
//        if (!(values is CacheValues)) {
//            throw SchemaSimulatorException("values passed to action must be of type CacheIndexes")
//        }
//
//        // Unpack needed variables
//        val cacheId = values.cacheId
//        val sliceAt = values.sliceAt
//        val items = values.items
//        val position = values.position
//
//        // Create push operation
//        val pushOperation = Document(mapOf(
//            "data" to mutableMapOf(
//                "\$each" to items,
//                "\$slice" to sliceAt.unaryMinus()
//            )
//        ))
//
//        // We provided a position for adding the items in the cache
//        if (position != null) {
//            val data = pushOperation["data"] as MutableMap<String, Any>
//            data["\$position"] = position
//        }
//
//        val result = cache.updateOne(
//            Document(mapOf(
//                "_id" to cacheId
//            )),
//            Document(mapOf(
//                "\$push" to pushOperation
//            )))
//
//        if (result.isModifiedCountAvailable && result.modifiedCount == 0L) {
//            throw SchemaSimulatorException("failed to push items to cache object with id $cacheId")
//        }
//
//        return mapOf()
//    }
//}
