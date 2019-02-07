package com.mtools.schemasimulator.schemas.accounttransaction

import com.mongodb.MongoClient
import com.mongodb.client.ClientSession
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.Decimal128
import java.math.BigDecimal

class Account(logEntry: LogEntry,
              private val client: MongoClient,
              private val accounts: MongoCollection<Document>,
              private val transactions: MongoCollection<Document>,
              val name: String, var balance: BigDecimal) : Scenario(logEntry) {
    override fun indexes(): List<Index> {
        return listOf(
            Index(accounts.namespace.databaseName, accounts.namespace.collectionName,
                Indexes.ascending("name")
            )
        )
    }

    /*
     * Create a new account document
     */
    fun create() = log("create") {
        val result = accounts.updateOne(
            Document(mapOf(
                "name" to name
            )),
            Document(mapOf(
                "\$setOnInsert" to mapOf(
                    "name" to name,
                    "balance" to balance,
                    "pendingTransactions" to listOf<Document>()
                )
            )),
            UpdateOptions().upsert(true))
        if (result.upsertedId == null) {
            throw SchemaSimulatorException("failed to create account entry")
        }
    }

    /*
     * Transfer an amount to this account from the provided account
     */
    fun transfer(toAccount: Account, amount: BigDecimal) = log("transfer") {
        val session = client.startSession()

        try {
            // Start the transaction
            session.startTransaction()
            // Debit current transaction
            debit(session, amount)
            // Credit the target account
            toAccount.credit(session, amount)
            // Create a transaction entry
            Transaction(logEntry, transactions, this, toAccount, amount).create(session)
            // Execute the transaction
            session.commitTransaction()
        } catch (exception:Exception) {
            // Abort the transaction
            session.abortTransaction()
            // Rethrow the exception
            throw exception
        }
    }

    private fun credit(session: ClientSession, amount: BigDecimal) = log("credit") {
        val result = accounts.updateOne(session, Document(mapOf(
            "name" to name
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to credit account $name the amount $amount")
        }
    }

    private fun debit(session: ClientSession, amount: BigDecimal) = log("debit"){
        val result = accounts.updateOne(session, Document(mapOf(
            "name" to name,
            "balance" to mapOf(
                "\$gte" to amount
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount.unaryMinus()
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to debit account ${this.name} the amount $amount")
        }
    }

    fun reload() = log("reload") {
        val doc = accounts.find(Document(mapOf(
            "name" to name
        ))).firstOrNull()

        doc ?: throw SchemaSimulatorException("failed to reload account $name")
        val value = doc["balance"]

        if (value is Decimal128) {
            balance = value.bigDecimalValue()
        }
    }
}

class Transaction(
    logEntry: LogEntry,
    private val transactions: MongoCollection<Document>,
    private val fromAccount: Account,
    private val toAccount: Account,
    private val amount: BigDecimal) : Scenario(logEntry) {
    override fun indexes(): List<Index> {
        return listOf()
    }

    fun create(session: ClientSession) = log("create") {
        transactions.insertOne(session, Document(mapOf(
            "source" to fromAccount.name,
            "destination" to toAccount.name,
            "amount" to amount
        )))
    }
}
