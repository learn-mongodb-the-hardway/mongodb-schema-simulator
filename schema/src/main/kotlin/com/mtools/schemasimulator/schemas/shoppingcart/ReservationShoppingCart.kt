package com.mtools.schemasimulator.schemas.shoppingcart

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.schemas.Scenario
import org.bson.Document
import java.util.*

abstract class ShoppingCart(val db: MongoDatabase): Scenario {

    fun setup() {}

    fun teardown() {}
}

interface Action {
    fun execute(values: Map<String, Any>)
}

class AddProductToShoppingCart(private val carts: MongoCollection<Document>,
                               private val inventories: MongoCollection<Document>) : Action {
    override fun execute(values: Map<String, Any>) {
        if (!values.containsKey("userId")
            || !values.containsKey("quantity")
            || !values.containsKey("product")) {
            throw SechemaSimulatorException("a userId, product and quantity must be passed into AddProductToShoppingCart")
        }

        // Attempt to Add product to shopping cart, nothing to do if it fails
        AddProductToShoppingCartDocument(carts).execute(values)
        // Attempt to update the inventory
        try {
            ReserveProductToInventoryDocument(inventories).execute(values)
        } catch (exception: SechemaSimulatorException) {
            // We have to roll back the shopping cart
            RollBackShoppingCartDocument(carts).execute(values)
        }
    }
}

class RollBackShoppingCartDocument(
    private val carts: MongoCollection<Document>) : Action {

    override fun execute(values: Map<String, Any>) {
        if (!values.containsKey("userId") || !values.containsKey("product")) {
            throw SechemaSimulatorException("a userId and product must be passed into RollBackShoppingCartDocument")
        }

        // Unwrap needed values
        val document = values["product"] as Document

        // Execute cart update
        val result = carts.updateOne(Document(mapOf(
            "_id" to values["userId"]
        )), Document(mapOf(
            "\$set" to mapOf("modifiedOn" to Date()),
            "\$pull" to mapOf(
                "products" to mapOf(
                    "_id" to document["productId"]
                )
            ))))

        if (result.modifiedCount == 1L) {
            return
        }

        throw SechemaSimulatorException("Failed to rollback shopping cart")
    }
}

class AddProductToShoppingCartDocument(
    private val carts: MongoCollection<Document>) : Action {

    override fun execute(values: Map<String, Any>) {
        if (!values.containsKey("userId") || !values.containsKey("product")) {
            throw SechemaSimulatorException("a userId and product must be passed in into AddProductToShoppingCartDocument")
        }

        // Unwrap needed values
        val document = values["product"] as Document

        // Execute cart update
        val result = carts.updateOne(Document(mapOf(
            "_id" to values["userId"],
            "state" to "active"
        )), Document(mapOf(
            "\$set" to mapOf("modifiedOn" to Date()),
            "\$push" to mapOf(
                "products" to mapOf(
                     "_id" to document["productId"],
                     "quantity" to document["quantity"],
                     "name" to document["name"],
                     "price" to document["price"]
                )
            )
        )), UpdateOptions().upsert(true))

        if (result.upsertedId != null || result.modifiedCount == 1L) {
            return
        }

        throw SechemaSimulatorException("Failed to upsert to update the shopping cart")
    }
}

class ReserveProductToInventoryDocument(
    private val inventories: MongoCollection<Document>) : Action {

    override fun execute(values: Map<String, Any>) {
        if (!values.containsKey("userId")
            || !values.containsKey("quantity")
            || !values.containsKey("product")) {
            throw SechemaSimulatorException("a userId, product and quantity must be passed into ReserveProductToInventoryDocument")
        }

        // Unwrap needed values
        val document = values["product"] as Document
        val quantity = values["quantity"] as Int

        // Execute the inventory update
        val result = inventories.updateOne(Document(mapOf(
            "_id" to document["_id"],
            "quantity" to mapOf("\$gte" to quantity)
        )), Document(mapOf(
            "\$inc" to mapOf("quantity" to quantity.unaryMinus()),
            "\$push" to mapOf(
                "reservations" to mapOf(
                    "quantity" to quantity,
                    "_id" to values["userId"],
                    "createdOn" to Date()
                )
            ))), UpdateOptions().upsert(false))

        if (result.modifiedCount == 1L) {
            return
        }

        throw SechemaSimulatorException("Failed to update the product inventory")
    }
}

class SechemaSimulatorException(message: String): Exception(message)

