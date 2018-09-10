package com.mtools.schemasimulator.schemas.account

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal

enum class TransactionFailPoints {
    NONE,
    FAIL_BEFORE_APPLY,
    FAIL_AFTER_FIRST_APPLY,
    FAIL_AFTER_APPLY,
    FAIL_AFTER_COMMIT
}

class Account(logEntry: LogEntry,
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
    fun transfer(toAccount: Account, amount: BigDecimal, failurePoint: TransactionFailPoints = TransactionFailPoints.NONE) : Transaction {
        var result:Transaction? = null

        log("transfer") {
            val transaction = Transaction(logEntry, accounts, transactions, ObjectId(), this, toAccount, amount)
            // Create transaction
            transaction.create()
            // Apply the transaction
            transaction.settle(failurePoint)
            // Set the transaction to return
            result = transaction
        }

        return result!!
    }

    /*
     * Debit the account with the specified amount
     */
    fun debit(transactionId: ObjectId, amount: BigDecimal) = log("debit") {
        val result = accounts.updateOne(Document(mapOf(
            "name" to name,
            "pendingTransactions" to mapOf(
                "\$ne" to transactionId
            ),
            "balance" to mapOf(
                "\$gte" to amount
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount.unaryMinus()
            ),
            "\$push" to mapOf(
                "pendingTransactions" to transactionId
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to debit account ${this.name} the amount $amount")
        }
    }

    fun credit(transactionId: ObjectId, amount: BigDecimal) = log("credit") {
        val result = accounts.updateOne(Document(mapOf(
            "name" to name,
            "pendingTransactions" to mapOf(
                "\$ne" to transactionId
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount
            ),
            "\$push" to mapOf(
                "pendingTransactions" to transactionId
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to credit account $name the amount $amount")
        }
    }

    fun clear(transactionId: ObjectId) = log("clear") {
        val result = accounts.updateOne(Document(mapOf(
            "name" to name
        )), Document(mapOf(
            "\$pull" to mapOf(
                "pendingTransactions" to transactionId
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to clear pending transaction $transactionId from account $name")
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
    val accounts: MongoCollection<Document>,
    val transactions: MongoCollection<Document>,
    val id: ObjectId,
    val fromAccount: Account,
    val toAccount: Account,
    val amount: BigDecimal) : Scenario(logEntry) {
    override fun indexes(): List<Index> {
        return listOf()
    }

    var state: TransactionStates = TransactionStates.UNKNOWN

    enum class TransactionStates {
        INITIAL,
        PENDING,
        COMMITTED,
        DONE,
        CANCELED,
        UNKNOWN
    }


    fun create() = log("clear") {
        transactions.insertOne(Document(mapOf(
            "_id" to id,
            "source" to fromAccount.name,
            "destination" to toAccount.name,
            "amount" to amount,
            "state" to TransactionStates.INITIAL.toString()
        )))

        state = TransactionStates.INITIAL
    }

    /*
     * Apply transaction to the accounts
     */
    fun settle(failurePoint: TransactionFailPoints = TransactionFailPoints.NONE) = log("settle") {
        // Advance the state of the transaction to pending
        try {
            advance()

            if (failurePoint == TransactionFailPoints.FAIL_BEFORE_APPLY) {
                throw Exception("failed to apply transaction")
            }
        } catch(ex: Exception) {
            cancel()
            throw SchemaSimulatorException("failed to advance transaction $id to state ${TransactionStates.PENDING}")
        }

        // Attempt to debit amount from the first account
        try {
            fromAccount.debit(id, amount)

            if (failurePoint == TransactionFailPoints.FAIL_AFTER_FIRST_APPLY) {
                throw Exception("failed to apply transaction to both accounts")
            }
        } catch(ex: Exception) {
            reverse()
            throw SchemaSimulatorException("failed to debit account ${fromAccount.name} with amount ${amount.unaryMinus()}")
        }

        // Attempt to credit the second account
        try {
            toAccount.credit(id, amount)

            if (failurePoint == TransactionFailPoints.FAIL_AFTER_APPLY) {
                throw Exception("failed after applying transaction to both accounts")
            }
        } catch(ex: Exception) {
            reverse()
            throw SchemaSimulatorException("failed to credit account ${toAccount.name} with amount $amount")
        }

        // Correctly set transaction to committed
        try {
            advance()
        } catch (ex: Exception) {
            reverse()
            throw SchemaSimulatorException("failed to advance transaction $id to state ${TransactionStates.COMMITTED}")
        }

        // Clear out the applied transaction on the first account
        fromAccount.clear(id)

        if (failurePoint == TransactionFailPoints.FAIL_AFTER_COMMIT) {
            throw Exception("failed when attempting to set transaction to committed")
        }

        // Clear out the applied transaction on the second account
        toAccount.clear(id)

        // Correctly set transaction to done
        try {
            advance()
        } catch (ex: Exception) {
            throw SchemaSimulatorException("failed to advance transaction $id to state ${TransactionStates.DONE}")
        }
    }

    /*
     * Reverse the transactions on the current account if it exists
     */
    fun reverse() = log("reverse") {
        // Reverse the debit
        accounts.updateOne(Document(mapOf(
            "name" to fromAccount.name,
            "pendingTransactions" to mapOf(
                "\$in" to listOf(id)
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount
            ),
            "\$pull" to mapOf(
                "pendingTransactions" to id
            )
        )))

        // Reverse the credit (if any)
        accounts.updateOne(Document(mapOf(
            "name" to toAccount.name,
            "pendingTransactions" to mapOf(
                "\$in" to listOf(id)
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "balance" to amount.unaryMinus()
            ),
            "\$pull" to mapOf(
                "pendingTransactions" to id
            )
        )))

        // Finally cancel the transaction
        cancel()
    }

    fun cancel() = log("cancel") {
        val result = transactions.updateOne(Document(mapOf(
            "_id" to id
        )), Document(mapOf(
            "\$set" to mapOf(
                "state" to TransactionStates.CANCELED.toString()
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("no transaction found for $id")
        }
    }

    fun advance() = log("advance") {
        state = when (state) {
            TransactionStates.INITIAL -> {
                val result = transactions.updateOne(Document(mapOf(
                    "_id" to id, "state" to TransactionStates.INITIAL.toString()
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "state" to TransactionStates.PENDING.toString()
                    )
                )))

                if(result.isModifiedCountAvailable && result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("no initial state transaction found for $id")
                }

                TransactionStates.PENDING
            }
            TransactionStates.PENDING -> {
                val result = transactions.updateOne(Document(mapOf(
                    "_id" to id, "state" to TransactionStates.PENDING.toString()
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "state" to TransactionStates.COMMITTED.toString()
                    )
                )))

                if(result.isModifiedCountAvailable && result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("no pending state transaction found for $id")
                }

                TransactionStates.COMMITTED
            }
            TransactionStates.COMMITTED -> {
                val result = transactions.updateOne(Document(mapOf(
                    "_id" to id, "state" to TransactionStates.COMMITTED.toString()
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "state" to TransactionStates.DONE.toString()
                    )
                )))

                if(result.isModifiedCountAvailable && result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("no done state transaction found for $id")
                }

                TransactionStates.DONE
            }
            else -> throw SchemaSimulatorException("unexpected transaction state transition [$state]")
        }
    }
}
