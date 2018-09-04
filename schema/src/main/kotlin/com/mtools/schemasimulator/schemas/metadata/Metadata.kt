package com.mtools.schemasimulator.schemas.metadata

import com.mongodb.ReadPreference
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.schemas.AcceptsReadPreference
import com.mtools.schemasimulator.schemas.Index
import com.mtools.schemasimulator.schemas.Scenario
import org.bson.Document

class Metadata(
    logEntry: LogEntry,
    val metadatas: MongoCollection<Document>,
    val id: Any,
    val metadata: Document
) : Scenario(logEntry), AcceptsReadPreference {
    private var readPreference: ReadPreference = ReadPreference.primary()

    override fun setReadPreference(preference: ReadPreference) {
        readPreference = preference
    }

    override fun indexes(): List<Index> {
        return listOf(
            Index(metadatas.namespace.databaseName, metadatas.namespace.collectionName,
                Indexes.compoundIndex(
                    Indexes.ascending("metadata.key"),
                    Indexes.ascending("metadata.value")
                )
            )
        )
    }

    /*
     * Create a new metadata document on mongodb
     */
    fun create() = log("create") {
        metadatas.insertOne(Document(mapOf(
            "_id" to id, "metadata" to metadata
        )))
    }

    /*
     * Search using metadata fields
     */
    fun findByFields(fields: Map<String, Any>) : List<Metadata> {
        var finalMetadata: List<Metadata> = listOf()

        log("findByFields") {
            val queryParts = fields.map {
                Document(mapOf(
                    "\$elemMatch" to mapOf(
                        "key" to it.key,
                        "value" to it.value
                    )
                ))
            }

            // Generate query
            val query = when (queryParts.size) {
                1 -> Document(mapOf(
                    "metadata" to queryParts.first()
                ))
                else -> Document(mapOf(
                    "metadata" to mapOf(
                        "\$all" to queryParts
                    )
                ))
            }

            // Execute the cursor
            finalMetadata = metadatas
                .withReadPreference(readPreference)
                .find(query)
                .map {
                    Metadata(logEntry, metadatas, it["_id"]!!, it["metadata"] as Document)
                }.toList()
        }

        return finalMetadata
    }
}
