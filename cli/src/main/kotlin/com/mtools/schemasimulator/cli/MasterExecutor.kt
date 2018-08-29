package com.mtools.schemasimulator.cli

import com.beust.klaxon.Klaxon
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.messages.slave.Register
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LocalSlaveTicker
import com.mtools.schemasimulator.load.MasterTicker
import com.mtools.schemasimulator.load.RemoteSlaveTicker
import com.mtools.schemasimulator.load.SlaveTicker
import com.mtools.schemasimulator.logger.NoopLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.io.Reader
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager

data class MasterExecutorConfig (
    val master: Boolean,
    val config: String,
    val host: String?,
    val port: Int?,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class MasterExecutor(val config: MasterExecutorConfig) : Executor {
    fun start() {
        var mongoClient: MongoClient?
        // Attempt to read the configuration Kotlin file
        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
        // Load the file
        val result = engine.eval(config.config)

        // Ensure the type is of Config
        if (!(result is Config)) {
            throw Exception("Configuration file must contain a config {} section at the end")
        }

        // Use the config to build out object structure
        try {
            mongoClient = MongoClient(MongoClientURI(result.mongo.url))
            mongoClient.listDatabaseNames().first()
        } catch(ex: MongoException) {
            logger.error { "Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}"}
            throw SystemExitException("Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}", 2)
        }

        // Metric Logger
        val metricLogger = NoopLogger()

        // Build the slave tickers
        val tickers = result.coordinator.tickers.map {
            when (it) {
                is LocalConfig -> {
                    val loadPatternConfig = it.loadPatternConfig

                    LocalSlaveTicker(it.name, mongoClient, when(loadPatternConfig) {
                        is ConstantConfig -> {
                            Constant(
                                ThreadedSimulationExecutor(it.simulation, metricLogger),
                                loadPatternConfig.numberOfCExecutions,
                                loadPatternConfig.executeEveryMilliseconds
                            )
                        }
                        else -> throw Exception("Unknown LoadPattern Config Object")
                    })
                }
                is RemoteConfig -> {
                    RemoteSlaveTicker(it.name)
                }
                else -> throw Exception("Unknown Load Generator Config")
            }
        }

        // Do we have the master flag enabled
        if (config.master) {
            val server = MasterSocketServer(config, LinkedBlockingDeque(tickers.toList()))
            server.start()
        }

        // Wait for all slaves to be ready
        var ready = false
        // Wait for all slave tickers to be armed
        while (!ready) {
            ready = tickers.map {
                it.ready()
            }.reduce { acc, value -> acc.and(value) }

            logger.info { "Wait for all slave tickers to ready" }
            // Wait before checking again
            Thread.sleep(config.waitMSBetweenReconnectAttempts)
        }

        // Build the structure
        val ticker = MasterTicker(
            tickers.toList(),
            result.coordinator.tickResolutionMiliseconds,
            result.coordinator.runForNumberOfTicks)

        // Start the ticker
        ticker.start()
        // Wait for it to finish
        ticker.join()
        // Execute the stop
        ticker.stop()
    }

    companion object : KLogging()
}

class MasterSocketServer(
    private val config: MasterExecutorConfig,
    private val slaveTickers: LinkedBlockingDeque<SlaveTicker>): WebSocketServer(InetSocketAddress(config.host, config.port!!)) {

    // Regular expressions
    private val registerMessage = """method"\s*:\s*"register"""".toRegex()

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(registerMessage)) {
            val register = Klaxon().parse<Register>(message)

            // We have to have at least a slave ticker
            if (slaveTickers.size > 0 && register != null) {
                // Keeps the reference to the non initialized ticker
                var nonInitializedTicker: SlaveTicker? = null

                // Locate a ticker that is not ready
                for (i in 0 until slaveTickers.size) {
                    val ticker = slaveTickers.remove()

                    // Is it not ready, lets use this one
                    if (ticker is RemoteSlaveTicker && ticker.initialized().not()) {
                        nonInitializedTicker = ticker
                        break
                    } else {
                        slaveTickers.push(ticker)
                    }
                }

                // Do we have a ticker we can associate the slave ticker process with
                if (nonInitializedTicker != null && nonInitializedTicker is RemoteSlaveTicker) {
                    nonInitializedTicker.initialize(register.host, register.port, config.config)
                }
            }
        }
    }

    override fun onStart() {
        logger.info { "starting server on: ${config.host}:${config.port}" }
    }

    override fun onError(conn: WebSocket?, ex: java.lang.Exception?) {
    }

    companion object : KLogging()
}
