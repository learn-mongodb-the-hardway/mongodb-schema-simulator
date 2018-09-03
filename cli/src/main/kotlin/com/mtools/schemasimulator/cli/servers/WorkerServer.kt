package com.mtools.schemasimulator.cli.servers

import com.beust.klaxon.Klaxon
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.WorkerExecutorConfig
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LoadPattern
import com.mtools.schemasimulator.logger.NoopLogger
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureErrorResponse
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.master.Stop
import com.mtools.schemasimulator.messages.master.Tick
import com.mtools.schemasimulator.messages.worker.StopResponse
import kotlinx.coroutines.experimental.Job
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.lang.Exception
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import javax.script.ScriptEngineManager

class WorkerServer(val config: WorkerExecutorConfig, val onClose: (s: WorkerServer) -> Unit): WebSocketServer(InetSocketAddress(config.uri.host, config.uri.port)) {
    private val configureMessage = """method"\s*:\s*"configure"""".toRegex()
    private val tickMessage = """method"\s*:\s*"tick"""".toRegex()
    private val stopMessage = """method"\s*:\s*"stop"""".toRegex()
    private var name: String = ""

    // Internal variables
    private val mongoClients = mutableMapOf<String, MongoClient>()
    private val localWorkers = mutableMapOf<String, LocalWorker>()

    // Attempt to read the configuration Kotlin file
    private val engine = ScriptEngineManager().getEngineByExtension("kts")!!

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: [${conn!!.remoteSocketAddress.address.hostAddress}]" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: [${conn!!.remoteSocketAddress.address.hostAddress}]" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.debug { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(configureMessage) && conn != null) {
            val configure = Klaxon().parse<Configure>(message)
            if (configure != null) {
                // Set the name
                name = configure.name

                logger.info { "[$name]: received config message, setup worker executor" }

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
                                logger.error { "[$name]: Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}"}
                                return conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: MongoClient failed to connect", 2)))
                            }
                        }

                        // Metric logger
                        val metricLogger = NoopLogger()

                        // We have the ticker we need to setup a local executor
                        val localTicker = LocalWorker(configure.name, mongoClients[configure.name]!!, when (tickerConfig.loadPatternConfig) {
                            is ConstantConfig -> {
                                Constant(
                                    ThreadedSimulationExecutor(tickerConfig.simulation, metricLogger),
                                    tickerConfig.loadPatternConfig.numberOfCExecutions,
                                    tickerConfig.loadPatternConfig.executeEveryMilliseconds
                                )
                            }
                            else -> throw Exception("[$name]: Unknown LoadPattern Config Object")
                        })

                        // Start the ticker
                        localTicker.start()

                        // Save the ticker
                        localWorkers[configure.name] = localTicker

                        // Send confirmation
                        return conn.send(Klaxon().toJsonString(ConfigureResponse(configure.id)))
                    }

                    return conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: Could not locate remote config", 2)))
                }

                logger.error { "[$name]: Configuration file not expected type"}
                conn.send(Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: Configuration file not expected type", 2)))
            }
        } else if (message != null && message.contains(tickMessage) && conn != null) {
            val tick = Klaxon().parse<Tick>(message)
            if (tick != null) {
                logger.debug { "[$name]: received tick message: ${tick.time}" }

                // For each ticker tick a tick
                localWorkers.values.forEach {
                    it.tick(tick.time)
                }
            }
        } else if (message != null && message.contains(stopMessage) && conn != null) {
            val stop = Klaxon().parse<Stop>(message)
            if (stop != null) {
                // For each ticker tick a tick
                localWorkers.forEach { key, value ->
                    logger.info { "[$name]: executing stop message for: [$key]" }
                    value.stop()
                }

                // Send a stop reply mesage (to notify master we are done)
                conn.send(Klaxon().toJsonString(StopResponse(stop.id)))
                // Close connection
                conn.close()
                // Wait for flushing to be done
                while (!conn.isFlushAndClose) {
                    Thread.sleep(1)
                }
                // Call shutdown hook
                onClose(this)
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

class LocalWorker(private val name: String, mongoClient: MongoClient, private val pattern: LoadPattern) {
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
        pattern.start()
    }

    fun stop() {
        running = false
        pattern.stop()

        // Wait for any lagging jobs to finish
        while (jobs.size > 0) {
            prueJobs()
            Thread.sleep(10)
        }
    }

    init {
        pattern.init(mongoClient)
    }

    fun tick(time: Long) {
        pattern.tick(time).forEach {
            jobs.add(it)
        }
    }

    companion object : KLogging()
}
