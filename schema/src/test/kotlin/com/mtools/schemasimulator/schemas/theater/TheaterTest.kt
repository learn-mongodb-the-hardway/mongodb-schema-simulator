package com.mtools.schemasimulator.schemas.theater

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.createLogEntry
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TheaterTest {
    @Test
    @DisplayName("Should correctly insert job into queue")
    fun test1() {
        // Create a new Theater
        val theater = Theater(createLogEntry(), theaters, sessions, ObjectId(), "The Royal", listOf(
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ))

        // Create a theater instance
        theater.create()

        // Add a session to the theater
        val session = theater.addSession("Action Movie 5", "Another action movie", Date(), Date(), BigDecimal(10))

        // Create a cart
        val cart = Cart(createLogEntry(), carts, sessions, theaters, receipts, ObjectId())
        cart.create()

        // Seats to reserve [y cord, x cord]
        val seats = listOf(
            listOf(1, 5), listOf(1, 6), listOf(1, 7)
        )

        // Reserve some seats at the movie
        cart.reserve(session, seats)

        // Reservation ok, checkout the cart
        cart.checkout()

        // Validate seat reservations
        validateSeats(sessions, session, seats, (session.seatsAvailable - seats.size))

        // Validate the cart
        validateCart(carts, cart, CartState.DONE, listOf(
            ExpectedReservation(seats, session.price?.multiply(BigDecimal(seats.size))!!)
        ))
    }

    @Test
    @DisplayName("Should correctly set up theater and session and book tickets but fail to reserve the tickets")
    fun test2() {
        // Create a new Theater
        val theater = Theater(createLogEntry(), theaters, sessions, ObjectId(), "The Royal", listOf(
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ))

        // Create a theater instance
        theater.create()

        // Add a session to the theater
        val session = theater.addSession("Action Movie 5", "Another action movie", Date(), Date(), BigDecimal(10))

        // Create a cart
        val cart = Cart(createLogEntry(), carts, sessions, theaters, receipts, ObjectId())
        cart.create()

        // Seats to reserve [y cord, x cord]
        val seats = listOf(
            listOf(1, 5), listOf(1, 6), listOf(1, 7)
        )

        // Reserve some seats at the movie
        cart.reserve(session, seats)

        // Reservation ok, checkout the cart
        cart.checkout()

        // Attempt to check out cart again
        val cart2 = Cart(createLogEntry(), carts, sessions, theaters, receipts, ObjectId())
        cart2.create()

        // Seats to reserve [y cord, x cord]
        val seats2 = listOf(
            listOf(1, 5), listOf(1, 6), listOf(1, 7)
        )

        try {
            cart2.reserve(session, seats2)
        } catch (ex: Exception) {
            assertEquals("could not reserve seats [[1, 5], [1, 6], [1, 7]]", ex.message)
        }

        // Validate the cart
        validateCart(carts, cart2, CartState.ACTIVE, listOf())
    }

    @Test
    @DisplayName("Should correctly set up theater and session and book tickets but fail to apply to cart as it is gone")
    fun test3() {
        // Create a new Theater
        val theater = Theater(createLogEntry(), theaters, sessions, ObjectId(), "The Royal", listOf(
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ))

        // Create a theater instance
        theater.create()

        // Add a session to the theater
        val session = theater.addSession("Action Movie 5", "Another action movie", Date(), Date(), BigDecimal(10))
        // Save all available seats
        val seatsAvailable = session.seatsAvailable

        // Create a cart
        val cart = Cart(createLogEntry(), carts, sessions, theaters, receipts, ObjectId())
        cart.create()

        // Seats to reserve [y cord, x cord]
        val seats = listOf(
            listOf(1, 5), listOf(1, 6), listOf(1, 7)
        )

        // Reserve some seats at the movie
        cart.reserve(session, seats)

        // Destroy the cart
        val r = carts.deleteOne(Document(mapOf("_id" to cart.id)))
        assertEquals(1, r.deletedCount)

        try {
            // Reservation ok, checkout the cart
            cart.checkout()
        } catch (ex: Exception) {
            assertEquals("could not locate cart with id ${cart.id.toString()}", ex.message)
        }

        // Reload the session
        session.reload()

        // Assertions
        assertEquals(seatsAvailable, session.seatsAvailable)
        // Go over all seats and ensure they are empty
        session.seats.forEach {
            it.forEach {
                assertEquals(0, it)
            }
        }
    }

    @Test
    @DisplayName("Should correctly find expired carts and remove any reservations in them")
    fun test4() {
        // Create a new Theater
        val theater = Theater(createLogEntry(), theaters, sessions, ObjectId(), "The Royal", listOf(
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            listOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ))

        // Create a theater instance
        theater.create()

        // Add a session to the theater
        val session = theater.addSession("Action Movie 5", "Another action movie", Date(), Date(), BigDecimal(10))
        // Save all available seats
        val seatsAvailable = session.seatsAvailable

        // Create a cart
        val cart = Cart(createLogEntry(), carts, sessions, theaters, receipts, ObjectId())
        cart.create()

        // Seats to reserve [y cord, x cord]
        val seats = listOf(
            listOf(1, 5), listOf(1, 6), listOf(1, 7)
        )

        // Reserve some seats at the movie
        cart.reserve(session, seats)

        // Force expire the cart
        val result = carts.updateOne(Document(mapOf(
            "_id" to cart.id
        )), Document(mapOf(
            "\$set" to mapOf(
                "state" to CartState.EXPIRED.toString()
            )
        )))

        assertEquals(1, result.modifiedCount)

        // Release all the carts that are expired
        Cart(createLogEntry(), carts, sessions, theaters, receipts).releaseExpired()

        // Reload the session
        session.reload()

        // Assertions
        assertEquals(seatsAvailable, session.seatsAvailable)
        // Go over all seats and ensure they are empty
        session.seats.forEach {
            it.forEach {
                assertEquals(0, it)
            }
        }
    }

    private fun validateCart(carts: MongoCollection<Document>, cart: Cart, state: CartState, reservations: List<ExpectedReservation>) {
        val doc = carts.find(Document(mapOf(
            "_id" to cart.id
        ))).firstOrNull()
        assertNotNull(doc)

        val _reservations = doc?.get("reservations") as List<Document>
        assertEquals(state.toString(), doc?.getString("state"))

        // Validate all the reservations in the cart
        _reservations.forEachIndexed { index, document ->
            assertEquals((document["total"] as Decimal128).bigDecimalValue(), reservations[index].total)
            assertEquals(document["seats"], reservations[index].seats)
        }
    }

    private fun validateSeats(sessions: MongoCollection<Document>, session: Session, seats: List<List<Int>>, seatsLeft: Int) {
        val doc = sessions.find(Document(mapOf(
            "_id" to session.id
        ))).firstOrNull()

        assertNotNull(doc)
        assertEquals(doc?.getInteger("seatsAvailable"), seatsLeft)

        val _seats = doc?.get("seats") as List<List<Int>>
        val _reservations = doc?.get("reservations") as List<Document>

        seats.forEach {
            assertEquals(1, _seats[it[0]][it[1]])
        }

        assertEquals(0, _reservations.size)
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var theaters: MongoCollection<Document>
        lateinit var sessions: MongoCollection<Document>
        lateinit var carts: MongoCollection<Document>
        lateinit var receipts: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            theaters = db.getCollection("theaters")
            sessions = db.getCollection("sessions")
            carts = db.getCollection("carts")
            receipts = db.getCollection("receipts")

            // Drop collection
            theaters.drop()
            sessions.drop()
            carts.drop()
            receipts.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }

}

data class ExpectedReservation(val seats: List<List<Int>>, val total: BigDecimal)
