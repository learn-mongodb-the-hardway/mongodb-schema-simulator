package com.mtools.schemasimulator.schemas.nestedcategories

import com.mongodb.ReadPreference
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.AcceptsReadPreference
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import com.mtools.schemasimulator.schemas.SchemaSimulatorException
import org.bson.BsonRegularExpression
import org.bson.Document
import org.bson.types.ObjectId
import org.jetbrains.kotlin.backend.common.pop

/*
 * Create a new category instance
 */
class Category(
    logEntry: LogEntry,
    private val categories: MongoCollection<Document>,
    val id: Any = ObjectId(),
    var name: String = "",
    var category: String = "",
    var parent: String = ""
) : Scenario(logEntry), AcceptsReadPreference {
    private var readPreference: ReadPreference = ReadPreference.primary()

    override fun setReadPreference(preference: ReadPreference) {
        readPreference = preference
    }

    init {
        // If no parent was passed in
        var paths = category.split("/")
        paths = paths.dropLast(1)

        // Rejoin
        parent = paths.joinToString("/")
        // Special case of the root
        if (parent == "" && category != "/") {
            parent = "/"
        }
    }

    override fun indexes(): List<Index> = listOf(
        Index(categories.namespace.databaseName, categories.namespace.collectionName,
            Indexes.ascending("category")
        ),
        Index(categories.namespace.databaseName, categories.namespace.collectionName,
            Indexes.ascending("parent")
        )
    )

    /*
     * Create a new mongodb category createWriteModel
     */
    fun create() = log("create") {
        categories.insertOne(Document(mapOf(
            "_id" to id,
            "name" to name,
            "category" to category,
            "parent" to parent
        )))
    }

    /*
     * Find all direct children categories of a provided category path
     */
    fun findAllDirectChildCategories(
        path: String,
        allowUsageOfCoveredIndex: Boolean = false) : List<Category> = find(
            "findAllDirectChildCategories",
            BsonRegularExpression("^$path$"),
            allowUsageOfCoveredIndex
        )

    /*
     * Find all children categories below the provided category path
     */
    fun findAllChildCategories(
        path: String,
        allowUsageOfCoveredIndex: Boolean = false) : List<Category> = find(
        "findAllChildCategories",
            BsonRegularExpression("^$path"),
            allowUsageOfCoveredIndex
        )

    private fun find(
        method: String,
        regexp: BsonRegularExpression,
        allowUsageOfCoveredIndex: Boolean = false) : List<Category> {
        var results = listOf<Category>()

        log(method) {
            var cursor = categories
                .withReadPreference(readPreference)
                .find(Document(mapOf(
                    "parent" to regexp
                )))

            if (allowUsageOfCoveredIndex) {
                cursor = cursor.projection(Document(mapOf(
                    "_id" to 0, "name" to 1, "category" to 1
                )))
            }

            results = cursor.map {
                Category(
                    logEntry,
                    categories,
                    it["_id"]!!,
                    it.getString("name"),
                    it.getString("category"),
                    it.getString("parent")
                )
            }.toList()
        }

        return results
    }

    /*
     * Find a specific category by it's path
     */
    fun findOne(path: String, allowUsageOfCoveredIndex: Boolean = false) : Category {
        var category: Category = Category(logEntry, categories)

        log("findOne") {
            var cursor = categories
                .withReadPreference(readPreference)
                .find(Document(mapOf(
                    "category" to path
                )))

            if (allowUsageOfCoveredIndex) {
                cursor = cursor.projection(Document(mapOf(
                    "_id" to 0, "name" to 1, "category" to 1
                )))
            }

            // Get the createWriteModel
            val doc = cursor.firstOrNull()
            doc ?: throw SchemaSimulatorException("could not locate category with path $path")

            // Return the category
            category = Category(
                logEntry,
                categories,
                doc["_id"]!!,
                doc.getString("name"),
                doc.getString("category"),
                doc.getString("parent")
            )
        }

        return category
    }

    /*
     * Reload the category information
     */
    fun reload() = log("reload") {
        val c = categories.find(Document(mapOf(
            "_id" to id
        ))).firstOrNull()

        c ?: throw SchemaSimulatorException("could not locate category with id $id")
        name = c.getString("name")
        category = c.getString("category")
        parent = c.getString("parent")
    }
}
