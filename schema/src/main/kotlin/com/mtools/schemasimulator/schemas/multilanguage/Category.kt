package com.mtools.schemasimulator.schemas.multilanguage

import com.mongodb.client.MongoCollection
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.Document

/*
 * Create a new category instance
 */
class Category(
    logEntry: LogEntry,
    private val categories: MongoCollection<Document>,
    private val products: MongoCollection<Document>,
    val id: Any,
    // Hash of all the names by local ('en-us') etc
    // { 'en-us': 'computers' }
    var names: Document = Document()
) : Scenario(logEntry) {
    override fun indexes(): List<Index> = listOf()

    fun addLocal(local: String, name: String) = log("addLocal") {
        // Update the category with the new local for the name
        var result = categories.updateOne(Document(mapOf(
            "_id" to id
        )), Document(mapOf(
            "\$set" to mapOf<String, String>(
                "names.$local" to name
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("could not modify category with id $id")
        }

        // Update all the products that have the category cached
        products.updateMany(Document(mapOf(
            "categories._id" to id
        )), Document(mapOf(
            "\$set" to mapOf(
                "categories.\$.names.$local" to name
            )
        )))
    }

    /*
     * Remove a new name local from the category, update relevant products
     */
    fun removeLocal(local: String) = log("removeLocal") {
        // Update the category with the new local for the name
        val result = categories.updateOne(Document(mapOf(
            "_id" to id
        )), Document(mapOf(
            "\$unset" to mapOf(
                "names.$local" to 1
            )
        )))

        if (result.isModifiedCountAvailable
            && result.modifiedCount == 0L) {
            throw SchemaSimulatorException("could not modify category with id $id")
        }

        // Update all the products that have the category cached
        products.updateMany(Document(mapOf(
            "categories._id" to id
        )), Document(mapOf(
            "\$unset" to mapOf(
                "categories.\$.names.$local" to 1
            )
        )))
    }

    /*
     * Create a new mongodb category createWriteModel
     */
    fun create() = log("create") {
        categories.insertOne(Document(mapOf(
            "_id" to id,
            "names" to names
        )))
    }

    /*
     * Reload the category information
     */
    fun reload() = log("reload") {
        val category = categories.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        category ?: throw SchemaSimulatorException("could not locate category with id $id")

        names = category["names"] as Document
    }
}
