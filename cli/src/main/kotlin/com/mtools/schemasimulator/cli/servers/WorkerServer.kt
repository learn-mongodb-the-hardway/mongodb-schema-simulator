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
import com.mtools.schemasimulator.logger.RemoteMetricLogger
import com.mtools.schemasimulator.logger.postMessage
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureErrorResponse
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.master.Done
import com.mtools.schemasimulator.messages.master.Stop
import com.mtools.schemasimulator.messages.master.Tick
import mu.KLogging
import spark.kotlin.Http
import spark.kotlin.RouteHandler
import spark.kotlin.ignite
import spark.kotlin.options
import java.lang.Exception
import javax.script.ScriptEngineManager
import javax.script.SimpleScriptContext

class WorkerServer(val config: WorkerExecutorConfig) {
    private var name: String = ""
    // Internal variables
    private val mongoClients = mutableMapOf<String, MongoClient>()
    private val localWorkers = mutableMapOf<String, LocalWorker>()

    // Attempt to read the configuration Kotlin file
    private val engine = ScriptEngineManager().getEngineByExtension("kts")!!
    // Http server
    private val http: Http = ignite()
        .port(config.uri.port)

    private fun handleConfigure(routeHandler: RouteHandler) : String {
        val configure = Klaxon().parse<Configure>(routeHandler.request.body())
        configure ?: return Klaxon().toJsonString(ConfigureErrorResponse(0, "[$name]: No post body found", 2))

        // Set the name
        name = configure.name

        // Create context
        val context = SimpleScriptContext()

        logger.info { "[$name]: received config message, setup worker executor" }

        // Load the scenario
        engine.eval(configure.configString, context)

        // Execute the configure method
        val result = engine.eval("configure()", context)

        // Set the logger
        val metricLogger = RemoteMetricLogger(configure.name, config.masterURI)

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
                        return Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: MongoClient failed to connect", 2))
                    }
                }

                // We have the ticker we need to setup a local executor
                val localTicker = LocalWorker(configure.name, mongoClients[configure.name]!!, when (tickerConfig.loadPatternConfig) {
                    is ConstantConfig -> {
                        Constant(
                            ThreadedSimulationExecutor(tickerConfig.simulation, metricLogger, name),
                            tickerConfig.loadPatternConfig.numberOfCExecutions,
                            tickerConfig.loadPatternConfig.executeEveryMilliseconds
                        )
                    }
                    else -> throw Exception("[$name]: Unknown LoadPattern Config Object")
                }, metricLogger)

                // Start the ticker
                localTicker.start()

                // Save the ticker
                localWorkers[configure.name] = localTicker

                // Send confirmation
                return Klaxon().toJsonString(ConfigureResponse(configure.id))
            }

            return Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: Could not locate remote config", 2))
        }

        logger.error { "[$name]: Configuration file not expected type"}
        return Klaxon().toJsonString(ConfigureErrorResponse(configure.id, "[$name]: Configuration file not expected type", 2))
    }

    fun start() {
        http.post("/configure", "application/json") {
            logger.info("========================= /configure")
            handleConfigure(this)
        }

        http.post("/tick") {
            logger.info("========================= /tick")
            handleTick(this)
        }

        http.post("/stop") {
            logger.info("========================= /stop")
            handleStop(this)
        }

        http.service.awaitInitialization()
    }

    private fun handleStop(routeHandler: RouteHandler): Any {
        val body = routeHandler.request.body()
        body ?: return ""

        val stop = Klaxon().parse<Stop>(body)
        stop ?: return ""

        // Wait for workers to finish
        Thread {
            // For each ticker tick a tick
            localWorkers.forEach { key, value ->
                logger.info { "[$name]: executing stop message for: [$key]" }
                value.stop()
            }

            // Send the worker done
            val response = postMessage(config.masterURI, "/worker/done", Klaxon().toJsonString(Done(config.uri.host, config.uri.port)))

            // Shut down http server
            http.stop()
            // Shut down mongo client
            mongoClients.forEach { _, u ->
                u.close()
            }
        }.start()

        return ""
    }

    private fun handleTick(routeHandler: RouteHandler): Any {
        val body = routeHandler.request.body()
        body ?: return ""

        val tick = Klaxon().parse<Tick>(body)
        tick ?: return ""

        logger.debug { "[$name]: received tick message: ${tick.time}" }

        // For each ticker tick a tick
        localWorkers.values.forEach {
            it.tick(tick.time)
        }

        return ""
    }

    companion object : KLogging()
}
