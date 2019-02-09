package com.mtools.schemasimulator.schemas.shoppingcartnoreservation

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import java.util.*
import kotlin.system.measureNanoTime

class ShoppingCart(
    logEntry: LogEntry,
    private val carts: MongoCollection<Document>,
    private val inventories: MongoCollection<Document>,
    private val orders: MongoCollection<Document>
) : Scenario(logEntry) {

    fun addProduct(userId: Any, quantity: Int, product: Document) = log("addProduct") {
        // Execute cart update
        val result = carts.updateOne(Document(mapOf(
            "_id" to userId,
            "state" to "active"
        )), Document(mapOf(
            "\$set" to mapOf("modifiedOn" to Date()),
            "\$push" to mapOf(
                "products" to mapOf(
                    "_id" to product["_id"],
                    "quantity" to quantity,
                    "name" to product["name"],
                    "price" to product["price"]
                )
            )
        )), UpdateOptions().upsert(true))

        if (result.upsertedId == null && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("Failed to upsert or update the shopping cart")
        }
    }

    fun updateProduct(userId: Any, quantity: Int, product: Document) = log("updateProduct") {
        // Attempt to update the cart with the new product quantity
        val result = carts.updateOne(Document(mapOf(
            "_id" to userId, "products._id" to product["_id"], "state" to "active"
        )), Document(mapOf(
            "\$set" to mapOf(
                "modifiedOn" to Date(),
                "products.$.quantity" to quantity
            ))
        ))

        if (result.isModifiedCountAvailable && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to update quantity $quantity of product ${product["_id"]} for cart [$userId]")
        }
    }

    fun checkout(userId: Any, name: String, address: String, payment: Document) = log("checkout") {
        val cart = carts.find(Document(mapOf(
            "_id" to userId,
            "state" to "active"
        ))).first()

        cart ?: throw SchemaSimulatorException("could not locate the cart for the user ${userId}")

        // If no entries in the cart don't allow checkout
        if (cart.contains("products") && cart["products"] is List<*> && (cart["products"] as List<*>).size == 0) {
            throw SchemaSimulatorException("cart for user ${userId} does not contain any products and cannot be checkout out")
        }

        // Go over all the products in the cart
        @Suppress("UNCHECKED_CAST")
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
            // Insert an order createWriteModel
            orders.insertOne(Document(mapOf(
                "userId" to userId,
                "createdOn" to Date(),
                "shipping" to mapOf(
                    "name" to name,
                    "address" to address
                ),
                "payment" to payment,
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
    }

    fun expireAllCarts(cutOffDate: Date) = log("expireAllCarts") {
        carts.find(Document(mapOf(
            "modifiedOn" to mapOf(
                "\$lte" to cutOffDate
            ),
            "state" to "active"
        ))).forEach { cart ->
            @Suppress("UNCHECKED_CAST")
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
    }

    override fun indexes() : List<Index> = listOf(
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
