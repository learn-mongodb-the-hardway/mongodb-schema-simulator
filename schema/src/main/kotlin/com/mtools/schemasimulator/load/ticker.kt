package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.messages.master.Tick
import kotlinx.coroutines.experimental.Job
import mu.KLogging
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
    fun ready() : Boolean
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

class LocalSlaveTicker(val name: String, mongoClient: MongoClient, pattern: LoadPattern): BaseSlaveTicker(pattern)  {
    override fun ready(): Boolean {
        return true
    }

    init {
        pattern.init(mongoClient)
    }

    override fun tick(time: Long) {
        pattern.execute(time).forEach {
            jobs.add(it)
        }
    }

    companion object : KLogging()
}

class RemoteSlaveTicker (
    val name: String
): SlaveTicker  {
    var initialized = false
    private var ready = false
    private lateinit var client: WebSocketConnectionClient

    override fun ready() : Boolean {
        return ready
    }

    override fun start() {
        logger.info("Slave ticker start")
    }

    override fun stop() {
        logger.info("Slave ticker stop")
    }

    override fun tick(time: Long) {
        logger.info("master tick $time")
        client.send(Tick(time))
    }

    fun initialized(): Boolean {
        return initialized
    }

    fun initialize(client: WebSocketConnectionClient) {
        this.client = client
        initialized = true
        ready = true
    }

    companion object : KLogging()
}
