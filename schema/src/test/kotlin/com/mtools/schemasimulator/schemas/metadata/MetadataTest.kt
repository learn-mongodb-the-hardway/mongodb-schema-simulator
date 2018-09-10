package com.mtools.schemasimulator.schemas.metadata

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class MetadataTest {

    @Test
    @DisplayName("Correctly random metadata and query by metadata field")
    fun test1() {
        val metadata1 = Metadata(LogEntry(""), metadatas, ObjectId(), listOf(
            Document(mapOf("key" to "name", "value" to "test image")),
            Document(mapOf("key" to "type", "value" to "image")),
            Document(mapOf("key" to "iso", "value" to 100))
        ))

        val metadata2 = Metadata(LogEntry(""), metadatas, ObjectId(), listOf(
            Document(mapOf("key" to "name", "value" to "test image 2")),
            Document(mapOf("key" to "type", "value" to "image")),
            Document(mapOf("key" to "iso", "value" to 200))
        ))

        metadata1.create()
        metadata2.create()

        var docs = metadata1.findByFields(mapOf("type" to "image"))
        assertEquals(2, docs.size)

        docs = metadata1.findByFields(mapOf("type" to "image", "iso" to 100))
        assertEquals(1, docs.size)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var metadatas: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            metadatas = db.getCollection("metadatas")

            // Drop collection
            metadatas.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
