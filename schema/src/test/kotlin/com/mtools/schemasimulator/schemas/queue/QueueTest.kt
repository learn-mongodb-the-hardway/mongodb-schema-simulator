package com.mtools.schemasimulator.schemas.queue

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class QueueTest {
    @Test
    @DisplayName("Should correctly insert job into queue")
    fun test1() {
        val queue = Queue(LogEntry(""), queues)

        // Add some items to the queue
        queue.publish(1, Document(mapOf("work" to 1)))
        queue.publish(5, Document(mapOf("work" to 2)))
        queue.publish(3, Document(mapOf("work" to 3)))

        // Test
        var work = queue.fetchByPriority()
        assertNotNull(work)
        assertEquals(5, work.document.getInteger("priority"))

        work = queue.fetchFIFO()
        assertNotNull(work)
        assertEquals(1, work.document.getInteger("priority"))
    }

    @Test
    @DisplayName("Should correctly insert job into topic and listen to it")
    fun test2() {
        val topic = Topic(LogEntry(""), db, topics, 100000, 1000)
        topic.create()

        // Add some items to the queue
        topic.publish(Document(mapOf("work" to 1)))
        topic.publish(Document(mapOf("work" to 2)))
        topic.publish(Document(mapOf("work" to 3)))

        // Listen to the topic
        topic.listen(true, null)

        // Consume all the topic documents
        var doc = topic.next()
        assertNotNull(doc)
        var payload = doc?.get("payload") as Document
        assertEquals(1, payload.getInteger("work"))

        doc = topic.next()
        assertNotNull(doc)
        payload = doc?.get("payload") as Document
        assertEquals(2, payload.getInteger("work"))

        doc = topic.next()
        assertNotNull(doc)
        payload = doc?.get("payload") as Document
        assertEquals(3, payload.getInteger("work"))

        doc = topic.next()
        assertNull(doc)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var queues: MongoCollection<Document>
        lateinit var topics: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            queues = db.getCollection("queues")
            topics = db.getCollection("topics")

            // Drop collection
            queues.drop()
            topics.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}
