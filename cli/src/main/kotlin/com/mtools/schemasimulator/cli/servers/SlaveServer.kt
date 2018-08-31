package com.mtools.schemasimulator.cli.servers

import com.beust.klaxon.Klaxon
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.SlaveExecutorConfig
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LocalSlaveTicker
import com.mtools.schemasimulator.logger.NoopLogger
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureErrorResponse
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.master.Tick
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.lang.Exception
import java.net.InetSocketAddress
import javax.script.ScriptEngineManager

class SlaveServer(val config: SlaveExecutorConfig): WebSocketServer(InetSocketAddress(config.uri.host, config.uri.port)) {
    private val configureMessage = """method"\s*:\s*"configure"""".toRegex()
    private val tickMessage = """method"\s*:\s*"tick"""".toRegex()
    private val mongoClients = mutableMapOf<String, MongoClient>()
    private val localSlaveTickers = mutableMapOf<String, LocalSlaveTicker>()

    // Attempt to read the configuration Kotlin file
    val engine = ScriptEngineManager().getEngineByExtension("kts")!!

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(configureMessage) && conn != null) {
            val configure = Klaxon().parse<Configure>(message)
            if (configure != null) {
                logger.info { "received config message, setup slave executor" }

                // Load the scenario
                val result = engine.eval(configure.configString)

                // Ensure the image
                if (result is Config) {
                    // Locate the ticker we named
                    val tickerConfig = result.coordinator.tickers
                        .filterIsInstance<RemoteConfig>()
                        .filter {
                        it.name == configure.name
                    }.firstOrNull()

                    // Ticker is null return an error
                    if (tickerConfig != null) {
                        // Check if there are clients available already otherwise add a new one
                        if (mongoClients.containsKey(configure.name).not()) {
                            try {
                                val mongoClient = MongoClient(MongoClientURI(result.mongo.url))
                                mongoClient.listDatabaseNames().first()
                                mongoClients[configure.name] = mongoClient
                            } catch(ex: MongoException) {
                                logger.error { "Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}"}
                                return conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "MongoClient failed to connect for ticker [${configure.name}]", 2)))
                            }
                        }

                        // Metric logger
                        val metricLogger = NoopLogger()

                        // We have the ticker we need to setup a local executor
                        val localTicker = LocalSlaveTicker(configure.name, mongoClients[configure.name]!!, when (tickerConfig.loadPatternConfig) {
                            is ConstantConfig -> {
                                Constant(
                                    ThreadedSimulationExecutor(tickerConfig.simulation, metricLogger),
                                    tickerConfig.loadPatternConfig.numberOfCExecutions,
                                    tickerConfig.loadPatternConfig.executeEveryMilliseconds
                                )
                            }
                            else -> throw Exception("Unknown LoadPattern Config Object")
                        })

                        // Start the ticker
                        localTicker.start()

                        // Save the ticker
                        localSlaveTickers[configure.name] = localTicker

                        // Send confirmation
                        return conn.send(Klaxon().toJsonString(ConfigureResponse(configure.id)))
                    }

                    return conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "Could not locate remote config named [${configure.name}]", 2)))
                }

                logger.error { "Configuration file not expected type"}
                conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "Configuration file not expected type", 2)))
            }
        } else if (message != null && message.contains(tickMessage) && conn != null) {
            val tick = Klaxon().parse<Tick>(message)
            if (tick != null) {
                logger.info { "received tick message: ${tick.time}" }

                // For each ticker execute a tick
                localSlaveTickers.forEach { key, value ->
                    logger.info { "executing tick message: [$key]:[${tick.time}]" }
                    value.tick(tick.time)
                }
            }
        }
    }

    override fun onStart() {
        logger.info { "starting server on: ${config.uri.host}:${config.uri.port}" }
    }

    override fun onError(conn: WebSocket?, ex: Exception?) {
    }

    companion object : KLogging()
}
