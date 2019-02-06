package com.mtools.schemasimulator.cli.workers

import com.mongodb.MongoClient
import com.mtools.schemasimulator.load.LoadPattern

class LocalWorker(
    private val name: String,
    private val mongoClient: MongoClient,
    private val pattern: LoadPattern
): Worker {
    override fun start(numberOfTicks: Long, tickResolution: Long) {
    }

    override fun ready() {}

    override fun init() {
        pattern.init(mongoClient)
    }

    override fun stop() {
        pattern.stop()
    }
}
