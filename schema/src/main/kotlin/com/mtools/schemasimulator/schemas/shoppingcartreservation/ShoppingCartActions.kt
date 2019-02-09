package com.mtools.schemasimulator.schemas.shoppingcartreservation

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document
import org.bson.types.ObjectId
import java.util.*
import kotlin.system.measureNanoTime

class ShoppingCart(
    logEntry: LogEntry,
    private val carts: MongoCollection<Document>,
    private val inventories: MongoCollection<Document>,
    private val orders: MongoCollection<Document>
) : Scenario(logEntry) {
    fun checkout(userId: Any, name: String, address: String, payment: Document) = log("checkout") {
        val cart = carts.find(Document(mapOf(
            "userId" to userId,
            "state" to "active"
        ))).first()

        cart ?: throw SchemaSimulatorException("could not locate the cart for the user $userId")

        // If no entries in the cart don't allow checkout
        if (cart.contains("products") && cart["products"] is List<*> && (cart["products"] as List<*>).size == 0) {
            throw SchemaSimulatorException("cart for user $userId does not contain any products and cannot be checkout out")
        }

        // Create an order Id
        val orderId = ObjectId()
        // Insert an order createWriteModel
        orders.insertOne(Document(mapOf(
            "_id" to orderId,
            "userId" to userId,
            "createdOn" to Date(),
            "shipping" to mapOf(
                "name" to name,
                "address" to address
            ),
            "payment" to payment,
            "products" to cart["products"]
        )))

        // Set the cart to complete
        var result = carts.updateOne(Document(mapOf(
            "_id" to cart["_id"]
        )), Document(mapOf(
            "\$set" to mapOf(
                "state" to "complete"
            )
        )))

        // If no createWriteModel was modified we failed to update the cart
        // and need to rollback
        if (result.modifiedCount == 0L) {
            // Rollback the order
            orders.deleteOne(Document(mapOf(
                "_id" to orderId
            )))

            throw SchemaSimulatorException("could not update cart for user $userId to complete")
        }

        // Pull the product reservation from all inventories
        result = inventories.updateMany(Document(mapOf(
            "reservations._id" to cart["_id"]
        )), Document(mapOf(
            "\$pull" to mapOf(
                "reservations" to mapOf(
                    "_id" to cart["_id"]
                )
            )
        )))

        // If no documents where modified, rollback
        // changes to the cart and delete the orders entry
        if (result.modifiedCount == 0L) {
            // Reactivate the cart
            carts.updateOne(Document(mapOf(
                "_id" to cart["_id"]
            )), Document(mapOf(
                "\$set" to mapOf(
                    "state" to "active"
                )
            )))

            // Rollback the order
            orders.deleteOne(Document(mapOf(
                "_id" to orderId
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
                var result = inventories.updateOne(Document(mapOf(
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

                if (result.modifiedCount == 0L) {
                    throw Exception("Failed to return product ${product["_id"]} from cart ${cart["_id"]} with quantity ${product["quantity"]} to inventory")
                }

                // Set the cart to expires
                result = carts.updateOne(Document(mapOf(
                    "_id" to cart["_id"]
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "state" to "expired"
                    )
                )))

                if (result.modifiedCount == 0L) {
                    throw Exception("Failed to set cart ${cart["_id"]} to expired")
                }
            }
        }
    }

    fun updateProduct(userId: Any, quantity: Int, product: Document) = log("updateProduct")  {
        // Unwrap needed values
        var oldQuantity = 0

        // Get the cart
        val cart = carts.find(Document(mapOf(
            "userId" to userId,
            "products._id" to product["_id"],
            "state" to "active"
        ))).first()

        // Throw id no cart found
        cart
            ?: throw SchemaSimulatorException("cart for user $userId not found")

        // Locate the product and get the old quantity
        @Suppress("UNCHECKED_CAST")
        val products = cart["products"] as List<Document>
        products.forEach {
            if (it["_id"] == product["_id"]) {
                oldQuantity = it["quantity"] as Int
            }
        }

        // Calculate the delta
        val delta = quantity - oldQuantity
        // Update the cart with new size
        var result = carts.updateOne(Document(mapOf(
            "userId" to userId,
            "products._id" to product["_id"],
            "state" to "active"
        )), Document(mapOf(
            "\$set" to mapOf(
                "modifiedOn" to Date(),
                "products.$.quantity" to quantity
            )
        )))

        // Fail to modify the cart
        if (result.modifiedCount == 0L) {
            throw SchemaSimulatorException("failed to modify cart for user $userId")
        }

        //
        // Update the inventory entries
        //

        // Update the cart with new size
        result = inventories.updateOne(Document(mapOf(
            "_id" to product["_id"],
            "reservations._id" to cart["_id"],
            "quantity" to mapOf(
                "\$gte" to delta
            )
        )), Document(mapOf(
            "\$inc" to mapOf(
                "quantity" to delta.unaryMinus()
            ),
            "\$set" to mapOf(
                "reservations.$.quantity" to quantity,
                "modifiedOn" to Date()
            )
        )))

        if (result.modifiedCount == 0L) {
            // Update the cart with new size
            result = carts.updateOne(Document(mapOf(
                "userId" to userId,
                "products._id" to product["_id"],
                "state" to "active"
            )), Document(mapOf(
                "\$set" to mapOf(
                    "products.\$.quantity" to oldQuantity,
                    "modifiedOn" to Date()
                )
            )))

            if (result.modifiedCount == 0L) {
                throw SchemaSimulatorException("Failed to rollback product ${product["_id"]} quantity $oldQuantity for user ${userId}")
            }
        }
    }

    fun addProduct(userId: Any, quantity: Int, product: Document) = log("addProduct")  {
        // Execute cart update
        var result = carts.updateOne(Document(mapOf(
            "userId" to userId,
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

        // Get the cart
        val cart = carts.find(Document(mapOf(
            "userId" to userId,
            "products._id" to product["_id"],
            "state" to "active"
        ))).first()

        // If we correctly inserted
        if ((result.upsertedId != null || result.modifiedCount == 1L) && cart != null) {
            // Execute the inventory update
            result = inventories.updateOne(Document(mapOf(
                "_id" to product["_id"],
                "quantity" to mapOf("\$gte" to quantity)
            )), Document(mapOf(
                "\$set" to mapOf("modifiedOn" to Date()),
                "\$inc" to mapOf("quantity" to quantity.unaryMinus()),
                "\$push" to mapOf(
                    "reservations" to mapOf(
                        "_id" to cart.getValue("_id"),
                        "quantity" to quantity,
                        "createdOn" to Date()
                    )
                ))), UpdateOptions().upsert(false))

            // Failed to update, rollback
            if (result.modifiedCount == 0L) {
                // Execute cart update
                result = carts.updateOne(Document(mapOf(
                    "userId" to userId
                )), Document(mapOf(
                    "\$set" to mapOf("modifiedOn" to Date()),
                    "\$pull" to mapOf(
                        "products" to mapOf(
                            "_id" to product["productId"]
                        )
                    ))))

                if (result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("Failed to rollback shopping cart")
                } else {
                    throw SchemaSimulatorException("Failed to update the product inventory")
                }
            }
        } else {
            throw SchemaSimulatorException("Failed to upsert to update the shopping cart")
        }
    }

    override fun indexes(): List<Index> = listOf(
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
