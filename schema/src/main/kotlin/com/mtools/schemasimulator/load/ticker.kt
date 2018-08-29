package com.mtools.schemasimulator.load

import com.beust.klaxon.Klaxon
import com.mongodb.MongoClient
import com.mtools.schemasimulator.messages.master.Configure
import com.xenomachina.argparser.SystemExitException
import kotlinx.coroutines.experimental.Job
import mu.KLogging
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.io.Reader
import java.lang.Exception
import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Base64

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
}

class RemoteSlaveTicker (
    val name: String,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000): SlaveTicker  {
    var initialized = false
    private var ready = false
    private lateinit var client: MasterToSlaveClient
    private var currentReconnectAttempts = maxReconnectAttempts

    override fun ready() : Boolean {
        return ready
    }

    override fun start() {
    }

    override fun stop() {
    }

    override fun tick(time: Long) {
//        pattern.execute(time).forEach {
//            jobs.add(it)
//        }
    }

    fun initialized(): Boolean {
        return initialized
    }

    fun initialize(host: String, port: Int, config: String) {
        client = MasterToSlaveClient(URI.create("http://$host:$port"), name, config, this)
        client.connect()

        // Attempt to reconnect if the client failed (TODO silly simple way)
        while(true) {
            if (currentReconnectAttempts == 0) {
                throw SystemExitException("Attempted to connect to master at [$host:$port] $maxReconnectAttempts times but failed", 2)
            }

            if (client.isClosed) {
                currentReconnectAttempts -= 1
                client.reconnect()
            } else if (client.isOpen) {
                currentReconnectAttempts = maxReconnectAttempts
            }

            Thread.sleep(waitMSBetweenReconnectAttempts)
        }
    }
}

private class MasterToSlaveClient(
    uri: URI,
    val name: String,
    val config: String,
    val remoteSlaveTicker: RemoteSlaveTicker): WebSocketClient(uri) {

    override fun onOpen(handshakedata: ServerHandshake?) {
        logger.info { "connection opened" }
        // Send the config message
        remoteSlaveTicker.initialized = true
        // Convert to Base64
        val base64String = String(Base64.getEncoder().encode(config.toByteArray()))
        // Send the message
        this.send(Klaxon().toJsonString(Configure(name, base64String)))
    }

    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from master: $code::$reason::$remote" }
    }

    override fun onMessage(message: String?) {
    }

    override fun onError(ex: Exception?) {
        logger.error { "connection error: $ex" }
    }

    companion object : KLogging()
}
