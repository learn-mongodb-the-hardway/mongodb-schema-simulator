package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import kotlinx.coroutines.experimental.Job

interface LoadPattern {
    fun start(client: MongoClient)
    fun tick(time: Long, client: MongoClient) : List<Job>
    fun stop(client: MongoClient)
}
