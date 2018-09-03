package com.mtools.schemasimulator.schemas.shoppingcartnoreservation

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Action
import com.mtools.schemasimulator.schemas.ActionValues
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.IndexClass
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import java.util.*

data class NoReservationShoppingCartValues(
    val userId: Int = 0,
    val name: String = "",
    val address: String = "",
    val quantity: Int = 0,
    val payment: Map<String, Any> = mapOf(),
    val product: Document = Document(),
    val cutOffDate: Date = Date(),
    val delta: Int = 0,
    val newQuantity: Int = 0,
    val oldQuantity: Int = 0
): ActionValues

class ShoppingCartIndexes(private val carts: MongoCollection<Document>,
                          private val inventories: MongoCollection<Document>,
                          private val orders: MongoCollection<Document>) : IndexClass {
    override val indexes = listOf(
        Index(inventories.namespace.databaseName, inventories.namespace.collectionName,
            Indexes.ascending("reservations._id")
        ),
        Index(carts.namespace.databaseName, carts.namespace.collectionName,
            Indexes.ascending("modifiedOn")
        ),
        Index(carts.namespace.databaseName, carts.namespace.collectionName,
            Indexes.ascending("state")
        )
    )
}

class CheckoutCart(logEntry: LogEntry, private val carts: MongoCollection<Document>,
                   private val inventories: MongoCollection<Document>,
                   private val orders: MongoCollection<Document>) : Action(logEntry) {
    override fun run(values: ActionValues): Map<String, Any> {
        if (!(values is NoReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        val cart = carts.find(Document(mapOf(
            "_id" to values.userId,
            "state" to "active"
        ))).first()

        cart ?: throw SchemaSimulatorException("could not locate the cart for the user ${values.userId}")

        // If no entries in the cart don't allow checkout
        if (cart.contains("products") && cart["products"] is List<*> && (cart["products"] as List<*>).size == 0) {
            throw SchemaSimulatorException("cart for user ${values.userId} does not contain any products and cannot be checkout out")
        }

        // Go over all the products in the cart
        val products = cart.get("products") as List<Document>
        val failedReservations = mutableListOf<Document>()
        val successfulReservations = mutableListOf<Document>()

        // Iterate over all the products and attempt to reserve them
        products.forEach { product ->
            val result = inventories.updateOne(Document(mapOf(
                "_id" to product["_id"],
                "quantity" to mapOf(
                    "\$gte" to product["quantity"]
                )
            )), Document(mapOf(
                "\$inc" to mapOf(
                    "quantity" to product.getInteger("quantity", 0).unaryMinus()
                ),
                "\$push" to mapOf(
                    "reservations" to mapOf(
                        "quantity" to product["quantity"],
                        "_id" to cart["_id"],
                        "createdOn" to Date()
                    )
                )
            )))

            if (result.isModifiedCountAvailable
                && result.modifiedCount == 0L) {
                failedReservations += product
            } else {
                successfulReservations += product
            }
        }

        // Do we have any failed product reservations, roll back
        if (failedReservations.size > 0) {
            successfulReservations.forEach { success ->
                inventories.updateOne(Document(mapOf(
                    "_id" to success["_id"],
                    "reservations._id" to cart["_id"]
                )), Document(mapOf(
                    "\$inc" to mapOf(
                        "quantity" to success["quantity"]
                    ),
                    "\$pull" to mapOf(
                        "reservations" to mapOf(
                            "_id" to cart["_id"]
                        )
                    )
                )))
            }
        } else {
            // Insert an order document
            orders.insertOne(Document(mapOf(
                "userId" to values.userId,
                "createdOn" to Date(),
                "shipping" to mapOf(
                    "name" to values.name,
                    "address" to values.address
                ),
                "payment" to values.payment,
                "products" to cart["products"]
            )))

            // Update the cart to complete state
            carts.updateOne(Document(mapOf(
                "_id" to cart["_id"], "state" to "active"
            )), Document(mapOf(
                "\$set" to mapOf(
                    "state" to "complete"
                )
            )))

            // Update the inventories pull
            inventories.updateMany(Document(mapOf(
                "reservations._id" to cart["_id"]
            )), Document(mapOf(
                "\$pull" to mapOf(
                    "reservations" to mapOf(
                        "_id" to cart["_id"]
                    )
                )
            )))
        }

        return mapOf()
    }
}

class UpdateReservationQuantityForAProduct(logEntry: LogEntry,
                  private val carts: MongoCollection<Document>) : Action(logEntry) {
    override fun run(values: ActionValues): Map<String, Any> {
        if (!(values is NoReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        // Unpack the values
        val newQuantity = values.quantity
        val userId = values.userId
        val product = values.product

        // Attempt to update the cart with the new product quantity
        val result = carts.updateOne(Document(mapOf(
            "_id" to userId, "products._id" to product["_id"], "state" to "active"
        )), Document(mapOf(
            "\$set" to mapOf(
                "modifiedOn" to Date(),
                "products.$.quantity" to newQuantity
            ))
        ))

        if (result.isModifiedCountAvailable && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to update quantity $newQuantity of product ${product["_id"]} for cart [$userId]")
        }

        return mapOf()
    }
}

class ExpireCarts(logEntry: LogEntry,
                  private val carts: MongoCollection<Document>,
                  private val inventories: MongoCollection<Document>) : Action(logEntry) {
    override fun run(values: ActionValues): Map<String, Any> {
        if (!(values is NoReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        carts.find(Document(mapOf(
            "modifiedOn" to mapOf(
                "\$lte" to values.cutOffDate
            ),
            "state" to "active"
        ))).forEach { cart ->
            val products = cart["products"] as List<Document>
            products.forEach { product ->

                // Return the quantity to the inventory from the cart
                inventories.updateOne(Document(mapOf(
                    "_id" to product["_id"],
                    "reservations._id" to cart["_id"],
                    "reservations.quantity" to product["quantity"]
                )), Document(mapOf(
                    "\$inc" to mapOf(
                        "quantity" to product["quantity"]
                    ),
                    "\$pull" to mapOf(
                        "reservations" to mapOf(
                            "_id" to cart["_id"]
                        )
                    )
                )))

                // Set the cart to expires
                carts.updateOne(Document(mapOf(
                    "_id" to cart["_id"]
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "state" to "expired"
                    )
                )))
            }
        }

        return mapOf()
    }
}

class AddProductToShoppingCart(logEntry: LogEntry,
                               private val carts: MongoCollection<Document>) : Action(logEntry) {

    override fun run(values: ActionValues): Map<String, Any> {
        if (!(values is NoReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        // Attempt to Add product to shopping cart, nothing to do if it fails
        AddProductToShoppingCartDocument(logEntry, carts).execute(values)
        return mapOf()
    }

    companion object {
        class AddProductToShoppingCartDocument(logEntry: LogEntry,
                                               private val carts: MongoCollection<Document>) : Action(logEntry) {

            override fun run(values: ActionValues): Map<String, Any> {
                if (!(values is NoReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product

                // Execute cart update
                val result = carts.updateOne(Document(mapOf(
                    "_id" to values.userId,
                    "state" to "active"
                )), Document(mapOf(
                    "\$set" to mapOf("modifiedOn" to Date()),
                    "\$push" to mapOf(
                        "products" to mapOf(
                            "_id" to document["_id"],
                            "quantity" to values.quantity,
                            "name" to document["name"],
                            "price" to document["price"]
                        )
                    )
                )), UpdateOptions().upsert(true))

                if (result.upsertedId != null || result.modifiedCount == 1L) {
                    return mapOf()
                }

                throw SchemaSimulatorException("Failed to upsert or update the shopping cart")
            }
        }
    }
}

