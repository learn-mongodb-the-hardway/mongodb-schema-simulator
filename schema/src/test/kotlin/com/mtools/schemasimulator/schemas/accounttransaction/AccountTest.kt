package com.mtools.schemasimulator.schemas.accounttransaction

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mtools.schemasimulator.createLogEntry
import com.mtools.schemasimulator.schemas.account.Account
import com.mtools.schemasimulator.schemas.account.Transaction
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
            accountA.transfer(accountB, BigDecimal(2000))
        } catch(ex: Exception) {
            assertTrue(ex.message!!.contains("failed to debit account Joe1 with amount -2000"))
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

