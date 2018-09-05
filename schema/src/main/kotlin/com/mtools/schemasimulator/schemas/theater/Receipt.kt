package com.mtools.schemasimulator.schemas.theater

import com.mongodb.client.MongoCollection
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import org.bson.Document
import java.util.*

class Receipt (
    logEntry: LogEntry,
    private val receipts: MongoCollection<Document>,
    private val reservations: List<Document>
) : Scenario(logEntry) {

    override fun indexes(): List<Index> = listOf()

    fun create() {
        receipts.insertOne(Document(mapOf(
            "createdOn" to Date(),
            "reservations" to reservations
        )))
    }
}
