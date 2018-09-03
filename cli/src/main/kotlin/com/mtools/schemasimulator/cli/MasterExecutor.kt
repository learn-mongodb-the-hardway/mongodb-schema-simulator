package com.mtools.schemasimulator.cli

import com.beust.klaxon.Klaxon
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LoadPattern
import com.mtools.schemasimulator.logger.NoopLogger
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.Stop
import com.mtools.schemasimulator.messages.master.Tick
import com.mtools.schemasimulator.messages.worker.Register
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.net.InetSocketAddress
import java.net.URI
import java.util.*
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager

data class MasterExecutorConfig (
    val master: Boolean,
    val uri: URI?,
    val config: String,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

interface Worker {
    fun ready()
    fun init()
    fun tick(time: Long)
    fun stop()
}

class RemoteWorker(private val name: String, private val config: MasterExecutorConfig): Worker {
    private val configureReplyMessage = """method"\s*:\s*"configure"""".toRegex()
    private val stopReplyMessage = """method"\s*:\s*"stop"""".toRegex()
    private var client: WebSocketConnectionClient? = null
    var initialized = false
    var stopped = false

    // Websocket onOpen method
    private val onOpen: (c: WebSocketConnectionClient) -> Unit = fun(c: WebSocketConnectionClient) {
        client = c
    }

    // Websocket onMessage
    private val onMessage: (client: WebSocketConnectionClient, message: String?) -> Unit = fun(_: WebSocketConnectionClient, message: String?) {
        logger.info { "onMessage [$message]" }

        // Process the replies
        if (message != null) {
            if (message.contains(configureReplyMessage)) {
                initialized = true
            } else if (message.contains(stopReplyMessage)) {
                stopped = true
            }
        }
    }

    override fun ready() {
        while(client == null) {
            Thread.sleep(10)
        }
    }

    override fun init() {
        // Fire the init message
        client!!.send(Configure(name, String(Base64.getEncoder().encode(config.config.toByteArray()))))

        // Wait for the response before we are done
        while (!initialized) {
            Thread.sleep(10)
        }
    }

    override fun tick(time: Long) {
        client!!.send(Tick(time))
    }

    override fun stop() {
        // Fire the init message
        client!!.send(Stop(name))

        // Wait for the response before we are done
        while (!stopped) {
            Thread.sleep(10)
        }

        // Shutdown the webclient
        client!!.disconnect()
    }

    fun assign(host: String, port: Int) {
        var localClient = WebSocketConnectionClient(
            URI.create("http://$host:$port"),
            config.maxReconnectAttempts,
            config.waitMSBetweenReconnectAttempts,
            onOpen,
            onMessage)

        // Start connection
        localClient.connect()
    }

    companion object : KLogging()
}

class LocalWorker(private val name: String, private val mongoClient: MongoClient, private val pattern: LoadPattern): Worker {
    override fun ready() {}

    override fun init() {
        pattern.init(mongoClient)
    }

    override fun tick(time: Long) {
        pattern.tick(time)
    }

    override fun stop() {
        pattern.stop()
    }
}

class MasterExecutorServer(config: MasterExecutorConfig): WebSocketServer(InetSocketAddress(config.uri?.host ?: "127.0.0.1", config.uri!!.port)) {
    private val registerMessage = """method"\s*:\s*"register"""".toRegex()
    lateinit var nonInitializedWorkers: LinkedBlockingDeque<RemoteWorker>
    var workers = LinkedBlockingDeque<RemoteWorker>()

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(registerMessage) && conn != null) {
            val register = Klaxon().parse<Register>(message)

            // Attempt to grab the first worker
            val worker = nonInitializedWorkers.takeFirst()

            // Register worker ready for work
            if (register != null && worker != null) {
                worker.assign(register.host, register.port)
                workers.add(worker)
            }
        }
    }

    override fun onStart() {
    }

    override fun onError(conn: WebSocket?, ex: java.lang.Exception?) {
    }

    companion object : KLogging()
}

class MasterExecutor(private val config: MasterExecutorConfig) : Executor {
    private val masterExecutorServer = MasterExecutorServer(config)

    fun start() {
        var mongoClient: MongoClient?
        // Attempt to read the configuration Kotlin file
        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
        // Read the string
        val configFileString = config.config
        // Load the file
        val result = engine.eval(configFileString)

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

        // For all executors processes wait
        val workers = result.coordinator.tickers.map {
            when (it) {
                is RemoteConfig -> {
                    RemoteWorker(it.name, config)
                }

                is LocalConfig -> {
                    LocalWorker(it.name, mongoClient, when(it.loadPatternConfig) {
                        is ConstantConfig -> {
                            Constant(
                                ThreadedSimulationExecutor(it.simulation, metricLogger),
                                it.loadPatternConfig.numberOfCExecutions,
                                it.loadPatternConfig.executeEveryMilliseconds
                            )
                        }
                        else -> throw Exception("Unknown LoadPattern Config Object")
                    })
                }

                else -> throw Exception("Unknown Worker Config")
            }
        }

        // Do we have the master flag enabled
        if (config.master) {
            masterExecutorServer.start()
        }

        // Register all the remote workers
        masterExecutorServer.nonInitializedWorkers = LinkedBlockingDeque(workers.filterIsInstance<RemoteWorker>())

        // Wait for all workers to be ready
        workers.forEach {
            it.ready()
        }

        // Once all worker are ready initialize them
        workers.forEach {
            it.init()
        }

        // Execute our ticker program
        var currentTick = 0
        var currentTime = 0L

        // Run for the number of tickets we are expecting
        while (currentTick < result.coordinator.runForNumberOfTicks) {
            // Send a tick to each worker
            workers.forEach {
                it.tick(currentTime)
            }

            // Wait for the resolution time
            Thread.sleep(result.coordinator.tickResolutionMiliseconds)

            // Update the current ticker and time
            currentTick += 1
            currentTime += result.coordinator.tickResolutionMiliseconds
        }

        // Wait for the workers to finish
        workers.forEach {
            it.stop()
        }

        logger.info { "Finished executing simulation, terminating" }
    }

    companion object : KLogging()
}
