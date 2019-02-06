package com.mtools.schemasimulator.cli.workers

import com.mongodb.MongoClient
import com.mtools.schemasimulator.cli.servers.LocalWorker
import com.mtools.schemasimulator.load.LoadPattern
import com.mtools.schemasimulator.logger.MetricLogger
import mu.KLogging

class LocalWorkerFacade(
    private val name: String,
    private val mongoClient: MongoClient,
    private val pattern: LoadPattern,
    private val metricLogger: MetricLogger
): Worker {
    private val localTicker = LocalWorker(name, mongoClient, pattern, metricLogger)
    private var thread: Thread? = null

    override fun start(numberOfTicks: Long, tickResolution: Long) {
        // Initialize the ticker
        localTicker.start()

        // Run the ticker
        thread = Thread {
            // Execute our ticker program
            var currentTick = 0
            var currentTime = 0L

            // Run for the number of tickets we are expecting
            while (currentTick < numberOfTicks) {
                // Perform a tick
                localTicker.tick(currentTime)

                // Wait for the resolution time
                Thread.sleep(tickResolution)

                // Update the current ticker and time
                currentTick += 1
                currentTime += tickResolution
            }
        }
        thread!!.start()

        logger.info { "[$name] worker started" }
    }

    override fun ready() {}

    override fun init() {}

    override fun stop() {
        if (thread != null) {
            thread!!.join()
        }

        localTicker.stop()
    }

    companion object : KLogging()
}
