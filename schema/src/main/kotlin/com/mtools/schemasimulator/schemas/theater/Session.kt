package com.mtools.schemasimulator.schemas.theater

import com.beust.klaxon.Klaxon
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.ObjectId
import java.math.BigDecimal
import java.util.*

class Session (
    logEntry: LogEntry,
    private val sessions: MongoCollection<Document>,
    private val theaters: MongoCollection<Document>,
    val id: Any,
    val theaterId: ObjectId? = null,
    val name: String? = null,
    val description: String? = null,
    val start: Date? = null,
    val end: Date? = null,
    val price: BigDecimal? = null
) : Scenario(logEntry) {
    override fun indexes(): List<Index> = listOf(
        Index(sessions.namespace.databaseName, sessions.namespace.collectionName,
            Indexes.ascending("reservations._id")
        )
    )

    /*
     *  Create a new session instance and save the document in mongodb
     */
    fun create(): Session {
        val doc = theaters.find(Document(mapOf(
            "_id" to theaterId
        ))).firstOrNull()

        doc ?: throw SchemaSimulatorException("no theater instance found for id $theaterId")

        // Create a session for this theater
        sessions.insertOne(Document(mapOf(
            "_id" to id,
            "theaterId" to theaterId,
            "name" to name,
            "description" to description,
            "start" to start,
            "end" to end,
            "price" to price,
            "seatsAvailable" to doc["seatsAvailable"],
            "seats" to doc["seats"],
            "reservations" to listOf<Document>()
        )))

        return this
    }

    /*
     *  Perform a reservation of a set of seats in this specific session
     */
    fun reserve(id: Any, seats: List<List<Int>>) {
        val seatsQuery = mutableListOf<Document>()
        val setSeatsSelection = Document()

        // Build the seats check
        seats.forEach {
            val seatSelector = Document()

            // Build the $and that ensures that we only reserve seats if they are all available
            seatSelector["seats.${it[0]}.${it[1]}"] = 0
            seatsQuery.add(seatSelector)
            // Set all the seats to occupied
            setSeatsSelection["seats.${it[0]}.${it[1]}"] = 1
        }

        // Attempt to reserve the seats
        val result = sessions.updateOne(Document(mapOf(
            "_id" to id,
            "theaterId" to theaterId,
            "\$and" to seatsQuery
        )), Document(mapOf(
            "\$set" to setSeatsSelection,
            "\$inc" to mapOf(
                "seatsAvailable" to seats.size.unaryMinus()
            ),
            "\$push" to mapOf(
                "reservations" to mapOf(
                    "_id" to id,
                    "seats" to seats,
                    "price" to price,
                    "total" to price?.multiply(BigDecimal(seats.size))
                )
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("could not reserve seats ${Klaxon().toJsonString(seats)}")
        }
    }

    /*
     * Release all the reservations for a cart across all sessions
     */
    fun releaseAll(id: Any) {
        val docs = sessions.find(Document(mapOf(
            "reservations._id" to id
        ))).toList()

        if (docs.isEmpty()) return

        // Reverses a specific reservation
        fun reverseReservation(doc: Document, id: Any) {
            // Locate the right cart id
            val reservations = doc["reservations"] as List<Document>
            val reservation = reservations.firstOrNull {
                it["_id"] == id
            }

            // No reservation found return
            reservation ?: return
            // Reverse the specific reservation
            Session(logEntry, sessions, theaters, id).release(reservation["_id"]!!, reservation["seats"] as List<List<Int>>)
        }

        // Process all the entries
        docs.forEach {
            // For each entry reverse the reservation for this cart
            reverseReservation(it, id)
        }
    }

    /*
     * Release a specific reservation and clear seats
     */
    fun release(id: Any, seats: List<List<Int>>) {
        val setSeatsSelection = Document()

        // Release all the seats
        seats.forEach {
            setSeatsSelection["seats.${it[0]}.${it[1]}"] = 0
        }

        // Remove the reservation
        val result = sessions.updateOne(Document(mapOf(
            "_id" to id
        )), Document(mapOf(
            "\$set" to setSeatsSelection,
            "\$pull" to mapOf(
                "reservations" to mapOf(
                    "_id" to id
                )
            )
        )))
    }

    /*
     * Apply all the reservations for a specific id across all sessions
     */
    fun settle(id: Any) {
        sessions.updateMany(Document(mapOf(
            "reservations._id" to id
        )), Document(mapOf(
            "\$pull" to mapOf(
                "reservations" to mapOf(
                    "_id" to id
                )
            )
        )))
    }
}





























