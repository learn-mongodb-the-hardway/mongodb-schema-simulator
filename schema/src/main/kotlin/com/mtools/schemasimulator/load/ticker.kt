package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import kotlinx.coroutines.experimental.Job
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * It's a synchronized load generator (a coordinator of action)
 * each tick will ping each load generator and let it decide what to apply of
 * load to the system
 **/
class MasterTicker(
    val slaveTickers: List<SlaveTicker> = listOf(),
    val tickResolutionMiliseconds: Long = 1L,
    val runForNumberOfTicks: Long = 1000L
) {
    // Internal state
    private var currentTime = 0L
    private var running = false
    private var currentTickNumber = 0L

    // Add to the current time
    fun addTime(time: Long) {
        synchronized(currentTime) {
            currentTime += time
        }
    }

    // Thread that runs the load patterns
    var thread: Thread = Thread(Runnable {
        while(running) {
            if (currentTickNumber >= runForNumberOfTicks) {
                break
            }

            // For each of the slave tickers signal them
            slaveTickers.forEach {
                it.tick(currentTime)
            }

            // Sleep for out tick resolutions
            Thread.sleep(tickResolutionMiliseconds)
            // Adjust the time
            currentTime += tickResolutionMiliseconds
            // Adjust the ticker count
            currentTickNumber += 1L
        }
    })

    fun start() {
        // Start all the slave tickers
        slaveTickers.forEach { it.start() }
        // Start main thread
        thread.start()
        running = true
    }

    fun stop() {
        // Start all the slave tickers
        slaveTickers.forEach { it.stop() }
        // Signal that we are stopping
        running = false
    }

    fun join() {
        thread.join()
    }
}

interface SlaveTicker {
    fun start()
    fun tick(time: Long)
    fun stop()
}

abstract class BaseSlaveTicker(val pattern: LoadPattern): SlaveTicker {
    val jobs = ConcurrentLinkedQueue<Job>()
    var running = false

    // Do cleanup of any jobs in the list that are done
    var thread = Thread(Runnable {
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

    override fun start() {
        running = true
        thread.start()
        pattern.start()
    }

    abstract override fun tick(time: Long)

    override fun stop() {
        running = false
        pattern.stop()

        // Wait for any lagging jobs to finish
        while (jobs.size > 0) {
            prueJobs()
            Thread.sleep(10)
        }
    }
}

class LocalSlaveTicker(pattern: LoadPattern, uri: String): BaseSlaveTicker(pattern)  {
    init {
        pattern.init(MongoClient(MongoClientURI(uri)))
    }

    override fun tick(time: Long) {
        pattern.execute(time).forEach {
            jobs.add(it)
        }
    }
}
