package com.mtools.schemasimulator.cli.servers

import com.mongodb.MongoClient
import com.mtools.schemasimulator.load.LoadPattern
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.logger.RemoteMetricLogger
import kotlinx.coroutines.experimental.Job
import mu.KLogging
import java.util.concurrent.ConcurrentLinkedQueue

class LocalWorker(
    private val name: String,
    private val mongoClient: MongoClient,
    private val pattern: LoadPattern,
    private val metricLogger: MetricLogger
) {
    private val jobs = ConcurrentLinkedQueue<Job>()
    private var running = false

    // Do cleanup of any jobs in the list that are done
    private var thread = Thread(Runnable {
        while(running) {
            prueJobs()
        }
    })

    private fun prueJobs() {
        jobs.forEach {
            if (it.isCancelled || it.isCompleted) {
                jobs.remove(it)
            }
        }
    }

    fun start() {
        running = true
        thread.start()
        pattern.start(mongoClient)
    }

    fun stop() {
        running = false
        pattern.stop(mongoClient)

        // Wait for any lagging jobs to finish
        while (jobs.size > 0) {
            prueJobs()
            Thread.sleep(10)
        }

        // Send any metrics left over
        metricLogger.flush()
    }

    fun tick(time: Long) {
        pattern.tick(time, mongoClient).forEach {
            jobs.add(it)
        }
    }

    companion object : KLogging()
}
