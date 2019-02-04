package com.mtools.schemasimulator.schemas.account

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.createLogEntry
import com.mtools.schemasimulator.logger.LogEntry
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AccountTest {

    @Test
    @DisplayName("Should correctly perform transfer between account A and account B of 100")
    fun test1() {
        val accountA = Account(createLogEntry(), accounts, transactions, "Joe", BigDecimal(1000))
        val accountB = Account(createLogEntry(), accounts, transactions, "Paul", BigDecimal(1000))

        accountA.create()
        accountB.create()

        val transaction = accountA.transfer(accountB, BigDecimal(100))

        accountA.reload()
        accountB.reload()

        val doc = transactions.find(Document(mapOf(
            "_id" to transaction.id
        ))).firstOrNull()
        assertNotNull(doc)
        assertTrue { doc?.get("state") is String }
        assertEquals(Transaction.TransactionStates.DONE.toString(), doc?.getString("state"))
    }

    @Test
    @DisplayName("Should correctly roll back transfer that fails before any application of amounts to accounts")
    fun test2() {
        val accountA = Account(createLogEntry(), accounts, transactions, "Joe1", BigDecimal(1000))
        val accountB = Account(createLogEntry(), accounts, transactions, "Paul1", BigDecimal(1000))

        accountA.create()
        accountB.create()

        // Fail transfer on apply
        try {
            accountA.transfer(accountB, BigDecimal(100), TransactionFailPoints.FAIL_BEFORE_APPLY)
        } catch(ex: Exception) {
            assertTrue(ex.message!!.contains("failed to advance transaction"))
        }

        accountA.reload()
        accountB.reload()

        assertEquals(BigDecimal(1000), accountA.balance)
        assertEquals(BigDecimal(1000), accountB.balance)

        val doc = transactions.find(Document(mapOf(
            "source" to accountA.name
        ))).firstOrNull()
        assertNotNull(doc)
        assertTrue { doc?.get("state") is String }
        assertEquals(Transaction.TransactionStates.CANCELED.toString(), doc?.getString("state"))
    }

    @Test
    @DisplayName("Should correctly roll back transfer that fails with only a single account being applied")
    fun test3() {
        val accountA = Account(createLogEntry(), accounts, transactions, "Joe2", BigDecimal(1000))
        val accountB = Account(createLogEntry(), accounts, transactions, "Paul2", BigDecimal(1000))

        accountA.create()
        accountB.create()

        // Fail transfer on apply
        try {
            accountA.transfer(accountB, BigDecimal(100), TransactionFailPoints.FAIL_AFTER_FIRST_APPLY)
        } catch(ex: Exception) {
            assertTrue(ex.message!!.contains("failed to debit account Joe2 with amount -100"))
        }

        accountA.reload()
        accountB.reload()

        assertEquals(BigDecimal(1000), accountA.balance)
        assertEquals(BigDecimal(1000), accountB.balance)

        val doc = transactions.find(Document(mapOf(
            "source" to accountA.name
        ))).firstOrNull()
        assertNotNull(doc)
        assertTrue { doc?.get("state") is String }
        assertEquals(Transaction.TransactionStates.CANCELED.toString(), doc?.getString("state"))
    }

    @Test
    @DisplayName("Should correctly roll back transfer that fails after application to accounts")
    fun test4() {
        val accountA = Account(createLogEntry(), accounts, transactions, "Joe3", BigDecimal(1000))
        val accountB = Account(createLogEntry(), accounts, transactions, "Paul3", BigDecimal(1000))

        accountA.create()
        accountB.create()

        // Fail transfer on apply
        try {
            accountA.transfer(accountB, BigDecimal(100), TransactionFailPoints.FAIL_AFTER_APPLY)
        } catch(ex: Exception) {
            assertTrue(ex.message!!.contains("failed to credit account Paul3 with amount 100"))
        }

        accountA.reload()
        accountB.reload()

        assertEquals(BigDecimal(1000), accountA.balance)
        assertEquals(BigDecimal(1000), accountB.balance)

        val doc = transactions.find(Document(mapOf(
            "source" to accountA.name
        ))).firstOrNull()
        assertNotNull(doc)
        assertTrue { doc?.get("state") is String }
        assertEquals(Transaction.TransactionStates.CANCELED.toString(), doc?.getString("state"))
    }

    @Test
    @DisplayName("Should correctly roll back transfer that fails after transaction set to commit but before clearing")
    fun test5() {
        val accountA = Account(createLogEntry(), accounts, transactions, "Joe4", BigDecimal(1000))
        val accountB = Account(createLogEntry(), accounts, transactions, "Paul4", BigDecimal(1000))

        accountA.create()
        accountB.create()

        // Fail transfer on apply
        try {
            accountA.transfer(accountB, BigDecimal(100), TransactionFailPoints.FAIL_AFTER_COMMIT)
        } catch(ex: Exception) {
            assertTrue(ex.message!!.contains("failed when attempting to set transaction to committed"))
        }

        accountA.reload()
        accountB.reload()

        assertEquals(BigDecimal(900), accountA.balance)
        assertEquals(BigDecimal(1100), accountB.balance)

        val doc = transactions.find(Document(mapOf(
            "source" to accountA.name
        ))).firstOrNull()
        assertNotNull(doc)
        assertTrue { doc?.get("state") is String }
        assertEquals(Transaction.TransactionStates.COMMITTED.toString(), doc?.getString("state"))
    }

    companion object {
        lateinit var client: MongoClient
        lateinit var db: MongoDatabase
        lateinit var accounts: MongoCollection<Document>
        lateinit var transactions: MongoCollection<Document>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            client = MongoClient(MongoClientURI("mongodb://localhost:27017"))
            db = client.getDatabase("integration_tests")
            accounts = db.getCollection("accounts")
            transactions = db.getCollection("transactions")

            // Drop collection
            accounts.drop()
            transactions.drop()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            client.close()
        }
    }
}

