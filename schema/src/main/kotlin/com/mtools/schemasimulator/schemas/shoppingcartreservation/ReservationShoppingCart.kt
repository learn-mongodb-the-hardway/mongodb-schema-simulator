package com.mtools.schemasimulator.schemas.shoppingcartreservation

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mtools.schemasimulator.schemas.Action
import com.mtools.schemasimulator.schemas.ActionValues
import org.bson.Document
import org.bson.types.ObjectId
import java.util.*

data class ReservationShoppingCartValues(
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

class CheckoutCart(private val carts: MongoCollection<Document>,
                   private val inventories: MongoCollection<Document>,
                   private val orders: MongoCollection<Document>) : Action {
    override fun execute(values: ActionValues): Map<String, Any> {
        if (!(values is ReservationShoppingCartValues)) {
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

        // Create an order Id
        val orderId = ObjectId()
        // Insert an order document
        orders.insertOne(Document(mapOf(
            "_id" to orderId,
            "userId" to values.userId,
            "createdOn" to Date(),
            "shipping" to mapOf(
                "name" to values.name,
                "address" to values.address
            ),
            "payment" to values.payment,
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

        // If no document was modified we failed to update the cart
        // and need to rollback
        if (result.modifiedCount == 0L) {
            // Rollback the order
            orders.deleteOne(Document(mapOf(
                "_id" to orderId
            )))

            throw SchemaSimulatorException("could not update cart for user ${values.userId} to complete")
        }

        // Pull the product reservation from all inventories
        result = inventories.updateMany(Document(mapOf(
            "reservations._id" to values.userId
        )), Document(mapOf(
            "\$pull" to mapOf(
                "reservations" to mapOf(
                    "_id" to values.userId
                )
            )
        )))

        // If no documents where modified, rollback
        // changes to the cart and delete the orders entry
        if (result.modifiedCount == 0L) {
            // Reactivate the cart
            var result = carts.updateOne(Document(mapOf(
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

        return mapOf()
    }
}

class ExpireCarts(private val carts: MongoCollection<Document>,
                  private val inventories: MongoCollection<Document>) : Action {
    override fun execute(values: ActionValues): Map<String, Any> {
        if (!(values is ReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        carts.find(Document(mapOf(
            "modifiedOn" to mapOf(
                "\$gt" to values.cutOffDate
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

class UpdateReservationQuantityForAProduct(private val carts: MongoCollection<Document>,
                                           private val inventories: MongoCollection<Document>) : Action {
    override fun execute(values: ActionValues): Map<String, Any> {
        if (!(values is ReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        // Update product quantity in cart
        val result1 = UpdateProductQuantityInCartDocument(carts).execute(values)
        val mergedOptions = ReservationShoppingCartValues(
            userId = values.userId, quantity = values.quantity, product = values.product,
            delta = result1["delta"] as Int, newQuantity = result1["newQuantity"] as Int, oldQuantity = result1["oldQuantity"] as Int
        )

        try {
            // Attempt to update inventory
            UpdateInventoryQuantityInCartDocument(inventories).execute(mergedOptions)
        } catch (exception: SchemaSimulatorException) {
            // Attempt to rollback
            RolbackCartQuantity(carts).execute(mergedOptions)
        }

        return mapOf()
    }

    companion object {
        class UpdateProductQuantityInCartDocument(private val carts: MongoCollection<Document>) : Action {
            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                var oldQuantity = 0
                val newQuantity = values.quantity
                val document = values.product

                // Get the cart
                val cart = carts.find(Document(mapOf(
                    "_id" to values.userId,
                    "products._id" to document["_id"],
                    "state" to "active"
                ))).first()

                // Throw id no cart found
                cart
                    ?: throw SchemaSimulatorException("cart for user ${values.userId} not found in UpdateProductQuantityInCartDocument")

                // Locate the product and get the old quantity
                val products = cart.get("products") as List<Document>
                products.forEach {
                    if (it.get("_id") == document["_id"]) {
                        oldQuantity = it.get("quantity") as Int
                    }
                }

                // Calculate the delta
                val delta = newQuantity - oldQuantity
                // Update the cart with new size
                val result = carts.updateOne(Document(mapOf(
                    "_id" to values.userId,
                    "products._id" to document["_id"],
                    "state" to "active"
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "modifiedOn" to Date(),
                        "products.$.quantity" to newQuantity
                    )
                )))

                // Failt to modify the cart
                if (result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("failed to modify cart for user ${values.userId}")
                }

                // Return the delta
                return mapOf(
                    "delta" to delta,
                    "newQuantity" to newQuantity,
                    "oldQuantity" to oldQuantity)
            }
        }

        class UpdateInventoryQuantityInCartDocument(private val inventories: MongoCollection<Document>) : Action {
            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product
                val delta = values.delta
                val newQuantity = values.newQuantity

                // Update the cart with new size
                val result = inventories.updateOne(Document(mapOf(
                    "_id" to document["_id"],
                    "reservations._id" to values.userId,
                    "quantity" to mapOf(
                        "\$gte" to delta
                    )
                )), Document(mapOf(
                    "\$inc" to mapOf(
                        "quantity" to delta.unaryMinus()
                    ),
                    "\$set" to mapOf(
                        "reservations.$.quantity" to newQuantity,
                        "modifiedOn" to Date()
                    )
                )))

                if (result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("Failed to reserve product ${document["_id"]} quantity $newQuantity for user ${values.userId}")
                }

                return mapOf()
            }
        }

        class RolbackCartQuantity(private val carts: MongoCollection<Document>) : Action {
            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product
                val oldQuantity = values.oldQuantity

                // Update the cart with new size
                val result = carts.updateOne(Document(mapOf(
                    "_id" to values.userId,
                    "products._id" to document["_id"],
                    "state" to "active"
                )), Document(mapOf(
                    "\$set" to mapOf(
                        "products.\$.quantity" to oldQuantity,
                        "modifiedOn" to Date()
                    )
                )))

                if (result.modifiedCount == 0L) {
                    throw SchemaSimulatorException("Failed to rollback product ${document["_id"]} quantity $oldQuantity for user ${values.userId}")
                }

                return mapOf()
            }
        }
    }
}

class AddProductToShoppingCart(private val carts: MongoCollection<Document>,
                               private val inventories: MongoCollection<Document>) : Action {

    override fun execute(values: ActionValues): Map<String, Any> {
        if (!(values is ReservationShoppingCartValues)) {
            throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
        }

        // Attempt to Add product to shopping cart, nothing to do if it fails
        AddProductToShoppingCartDocument(carts).execute(values)
        // Attempt to update the inventory
        try {
            ReserveProductToInventoryDocument(inventories).execute(values)
        } catch (exception: SchemaSimulatorException) {
            // We have to roll back the shopping cart
            RollBackShoppingCartDocument(carts).execute(values)
        }

        return mapOf()
    }

    companion object {
        class RollBackShoppingCartDocument(
            private val carts: MongoCollection<Document>) : Action {

            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product as Document

                // Execute cart update
                val result = carts.updateOne(Document(mapOf(
                    "_id" to values.userId
                )), Document(mapOf(
                    "\$set" to mapOf("modifiedOn" to Date()),
                    "\$pull" to mapOf(
                        "products" to mapOf(
                            "_id" to document["productId"]
                        )
                    ))))

                if (result.modifiedCount == 1L) {
                    return mapOf()
                }

                throw SchemaSimulatorException("Failed to rollback shopping cart")
            }
        }

        class AddProductToShoppingCartDocument(
            private val carts: MongoCollection<Document>) : Action {

            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product as Document

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

                throw SchemaSimulatorException("Failed to upsert to update the shopping cart")
            }
        }

        class ReserveProductToInventoryDocument(
            private val inventories: MongoCollection<Document>) : Action {

            override fun execute(values: ActionValues): Map<String, Any> {
                if (!(values is ReservationShoppingCartValues)) {
                    throw SchemaSimulatorException("values passed to action must be of type ReservationShoppingCartValues")
                }

                // Unwrap needed values
                val document = values.product as Document
                val quantity = values.quantity as Int

                // Execute the inventory update
                val result = inventories.updateOne(Document(mapOf(
                    "_id" to document["_id"],
                    "quantity" to mapOf("\$gte" to quantity)
                )), Document(mapOf(
                    "\$set" to mapOf("modifiedOn" to Date()),
                    "\$inc" to mapOf("quantity" to quantity.unaryMinus()),
                    "\$push" to mapOf(
                        "reservations" to mapOf(
                            "_id" to values.userId,
                            "quantity" to quantity,
                            "createdOn" to Date()
                        )
                    ))), UpdateOptions().upsert(false))

                if (result.modifiedCount == 1L) {
                    return mapOf()
                }

                throw SchemaSimulatorException("Failed to update the product inventory")
            }
        }
    }
}

class SchemaSimulatorException(message: String): Exception(message)

