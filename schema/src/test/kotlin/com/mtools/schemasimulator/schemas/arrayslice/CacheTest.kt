package com.mtools.schemasimulator.schemas.arrayslice

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.arraycache.Cache
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class CacheTest {

    @Test
    @DisplayName("Should correctly a 5 line cache no pre-allocation")
    fun test1() {
        val cache = Cache(LogEntry(""), caches, 5, ObjectId())
        cache.create()

        cache.push(listOf(
            Document(mapOf("a" to 1)), Document(mapOf("a" to 2)), Document(mapOf("a" to 3)),
            Document(mapOf("a" to 4)), Document(mapOf("a" to 5)), Document(mapOf("a" to 6))
        ))

        // Fetch the cache
        val doc = caches.find(Document(mapOf(
            "_id" to cache.id
        ))).firstOrNull()
        assertNotNull(doc)

        val data = doc?.get("data") as List<Document>
        assertEquals(5, data.size)
        assertEquals(2, data[0].getInteger("a"))
        assertEquals(6, data[4].getInteger("a"))
    }

    @Test
    @DisplayName("'Should correctly a 5 line cache with pre-allocation")
    fun test2() {
        val cache = Cache(LogEntry(""), caches, 3, ObjectId())
        cache.create(Document(mapOf(
            "a" to 1
        )))

        cache.push(listOf(
            Document(mapOf("a" to 1)), Document(mapOf("a" to 2)), Document(mapOf("a" to 3)),
            Document(mapOf("a" to 4))
        ))

        // Fetch the cache
        val doc = caches.find(Document(mapOf(
            "_id" to cache.id
        ))).firstOrNull()
        assertNotNull(doc)

        val data = doc?.get("data") as List<Document>
        assertEquals(3, data.size)
        assertEquals(2, data[0].getInteger("a"))
        assertEquals(4, data[2].getInteger("a"))
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var caches: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            caches = db.getCollection("cache")

            // Drop collection
            caches.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
