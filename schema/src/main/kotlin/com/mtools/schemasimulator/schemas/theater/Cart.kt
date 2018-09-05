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

enum class CartState {
    ACTIVE,
    DONE,
    CANCELED,
    EXPIRED
}

/*
 * Create a new cart instance
 */
class Cart (
    logEntry: LogEntry,
    private val carts: MongoCollection<Document>,
    private val sessions: MongoCollection<Document>,
    private val theaters: MongoCollection<Document>,
    private val receipts: MongoCollection<Document>,
    val id: ObjectId
) : Scenario(logEntry) {

    override fun indexes(): List<Index> = listOf(
        Index(carts.namespace.databaseName, carts.namespace.collectionName,
            Indexes.ascending("state")
        )
    )

    /*
     * Create a new cart
     */
    fun create() {
        carts.insertOne(Document(mapOf(
            "_id" to id,
            "state" to CartState.ACTIVE,
            "total" to 0,
            "reservations" to listOf<Document>(),
            "modifiedOn" to Date(),
            "createdOn" to Date()
        )))
    }

    /*
     * Attempt to reserve seats
     */
    fun reserve(session: Session, seats: List<List<Int>>) {
        // Reserve seats in the session
        session.reserve(id, seats)

        // Put reservation in the cart
        try {
            val result = carts.updateOne(Document(mapOf(
                "_id" to id
            )), Document(mapOf(
                "\$push" to mapOf(
                    "reservations" to mapOf(
                        "sessionId" to session.id,
                        "seats" to seats,
                        "price" to session.price,
                        "total" to session.price?.multiply(BigDecimal(seats.size))
                    )
                ),
                "\$inc" to mapOf(
                    "total" to session.price?.multiply(BigDecimal(seats.size))
                ),
                "\$set" to mapOf(
                    "modifiedOn" to Date()
                )
            )))

            if (result.isModifiedCountAvailable
                && result.modifiedCount == 0L) {
                // Release the seats in the session
                session.release(id, seats)
                throw SchemaSimulatorException("could not add seats to cart")
            }
        } catch (ex:Exception) {
            // Release the seats in the session
            session.release(id, seats)
            throw SchemaSimulatorException("could not add seats to cart")
        }
    }

    /*
     * Attempt to checkout the cart
     */
    fun checkout() {
        val doc = carts.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        if (doc == null) {
            // Cart is gone force clean all sessions for this cart
            Session(logEntry, sessions, theaters, id).releaseAll(id)
            throw SchemaSimulatorException("could not locate cart with id $id")
        }

        // Create Receipt
        Receipt(logEntry, receipts, doc["reservations"] as List<Document>).create()
        // Apply all reservations in the cart
        Session(logEntry, sessions, theaters, id).settle(id)
        // Update state of Cart to DONE
        val result = carts.updateOne(Document(mapOf(
            "_id" to id
        )), Document(mapOf(
            "\$set" to mapOf(
                "state" to CartState.DONE
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("could not find cart with id $id")
        }
    }

    /*
     * Release a reservation
     */
    fun release(reservation: Document) {
        Session(logEntry, sessions, theaters, reservation["sessionId"]!!)
            .release(id, reservation["seats"] as List<List<Int>>)
    }

    /*
     * Destroy the cart and cleanup
     */
    fun destroy() {
        // Fetch the cart
        val doc = carts.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        doc ?: throw SchemaSimulatorException("could not locate cart with id $id")

        // Reservations left
        val reservations = doc["reservations"] as List<Document>

        // For all the reservations, reverse them
        reservations.forEach {
            this.release(it)
        }
    }

    /*
     * Locate all expired carts and release all reservations
     */
    fun releaseExpired() {
        val docs = carts.find(Document(mapOf(
            "state" to CartState.EXPIRED
        ))).toList()

        if (docs.isEmpty()) return

        // Process each cart
        fun processCart(cart: Document) {
            // Release all reservations for this cart
            Session(logEntry, sessions, theaters, id)
                .releaseAll(cart["_id"]!!)
            // Set cart to expired
            carts.updateOne(Document(mapOf(
                "_id" to cart["_id"]
            )), Document(mapOf(
                "\$set" to mapOf(
                    "state" to CartState.CANCELED
                )
            )))
        }

        // Release all the carts
        docs.forEach {
            processCart(it)
        }
    }
}




















