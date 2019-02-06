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
import com.mtools.schemasimulator.messages.master.Start
import com.mtools.schemasimulator.messages.master.Stop
import mu.KLogging
import spark.kotlin.Http
import spark.kotlin.RouteHandler
import spark.kotlin.ignite
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

        logger.info { "[$name:${config.uri}]: received config message, setup worker executor" }

        // Load the scenario
        engine.eval(configure.configString, context)

        // Execute the configure method
        val result = engine.eval("configure()", context)

        // Set the logger
        val metricLogger = RemoteMetricLogger(configure.name, config.masterURI, config.uri)

        // Ensure the image
        if (result is Config) {
            // Locate the ticker we named
            val tickerConfig = result.coordinator.tickers
                .filterIsInstance<RemoteConfig>()
                .firstOrNull {
                    it.name == configure.name
                }

            // Ticker is null return an error
            if (tickerConfig != null) {
                // Check if there are clients available already otherwise add a new one
                if (mongoClients.containsKey(configure.name).not()) {
                    try {
                        val mongoClient = MongoClient(MongoClientURI(result.mongo.url))
                        mongoClient.listDatabaseNames().first()
                        mongoClients[configure.name] = mongoClient
                    } catch(ex: MongoException) {
                        logger.error { "[$name]: Failed to connect to MongoDB with masterURI [${result.mongo.url}, ${ex.message}"}
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
                Thread {
                    localTicker.start()
                }.start()

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
            handleConfigure(this)
        }

        http.post("/start") {
            handleStart(this)
        }

        http.service.awaitInitialization()
    }

    private fun handleStart(routeHandler: RouteHandler): Any {
        val body = routeHandler.request.body()
        body ?: return ""

        val start = Klaxon().parse<Start>(body)
        start ?: return ""

        logger.info { "[$name]: received start message: numberOfTicks={start.numberOfTicks}, tickResolution=${start.tickResolution}" }

        // Run the work
        Thread {
            // Execute our ticker program
            var currentTick = 0
            var currentTime = 0L

            // Run for the number of tickets we are expecting
            while (currentTick < start.numberOfTicks) {
                // Send a tick to each worker
                localWorkers.values.forEach {
                    it.tick(currentTime)
                }

                // Wait for the resolution time
                Thread.sleep(start.tickResolution)

                // Update the current ticker and time
                currentTick += 1
                currentTime += start.tickResolution
            }

            // Wait for work to finish
            // For each ticker tick a tick
            localWorkers.forEach { key, value ->
                logger.info { "[$name]: executing stop message for: [$key]" }
                value.stop()
            }

            // Send the worker done
            postMessage(config.masterURI, "/worker/done", Klaxon().toJsonString(Done(config.uri.host, config.uri.port)))

            // Shut down http server
            http.stop()
            // Shut down mongo client
            mongoClients.forEach { _, u ->
                u.close()
            }
        }.start()

        return ""
    }

    companion object : KLogging()
}
