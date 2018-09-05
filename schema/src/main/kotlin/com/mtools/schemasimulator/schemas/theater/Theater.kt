package com.mtools.schemasimulator.schemas.theater

import com.mongodb.client.MongoCollection
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import org.bson.Document
import org.bson.types.ObjectId
import java.math.BigDecimal
import java.util.*

class Theater (
    logEntry: LogEntry,
    private val theaters: MongoCollection<Document>,
    private val sessions: MongoCollection<Document>,
    val id: ObjectId,
    val name: String,
    val seats: List<List<Int>>
) : Scenario(logEntry) {

    override fun indexes(): List<Index> = listOf()

    /*
     *  Create a new theater instance
     */
    fun create(): Theater {
        theaters.insertOne(Document(mapOf(
            "_id" to id,
            "name" to name,
            "seats" to seats,
            "seatsAvailable" to seats.sumBy { it.size }
        )))

        return this
    }

    fun addSession(name: String, description: String, start: Date, end: Date, price: BigDecimal) : Session {
        return Session(
            logEntry,
            sessions,
            theaters,
            ObjectId(),
            id,
            name,
            description,
            start,
            end,
            price
        ).create()
    }

}
