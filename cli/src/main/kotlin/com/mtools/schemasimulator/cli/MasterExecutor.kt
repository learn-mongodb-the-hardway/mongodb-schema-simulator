package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.cli.workers.LocalWorkerFacade
import com.mtools.schemasimulator.logger.MetricsAggregator
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.executor.Simulation
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.logger.LocalMetricLogger
import com.mtools.schemasimulator.logger.MetricLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.jetbrains.kotlin.script.jsr223.KotlinJsr223JvmLocalScriptEngine
import java.net.URI
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager
import javax.script.SimpleScriptContext
import java.io.File

data class MasterExecutorConfig (
    val master: Boolean,
    val uri: URI?,
    val config: String,
    val maxReconnectAttempts: Int = 30,
    val graphOutputFilePath: File,
    val graphOutputDPI: Int = 300,
    val waitMSBetweenReconnectAttempts: Long = 1000,
    val configurationMethod: String = "configure",
    val graphFilters: List<String> = listOf(),
    val skipTicks: Int = 0)

class MasterExecutor(private val config: MasterExecutorConfig) : Executor {
    private val metricsAggregator = MetricsAggregator()
    private val masterExecutorServer = MasterExecutorServer(config, metricsAggregator)

    fun start() {
        var mongoClient: MongoClient?

        // Attempt to read the configuration Kotlin file
        val engine = ScriptEngineManager()
            .getEngineByExtension("kts")!! as KotlinJsr223JvmLocalScriptEngine

        // Read the string
        val configFileString = config.config

        // Create new context
        val context = SimpleScriptContext()

        // Load the Script
        try {
            engine.eval(configFileString, context)
        } catch (exception: Exception) {
            logger.error { "Failed to evaluate the simulation script" }
            throw exception
        }

        // Attempt to invoke simulation
        val result = engine.eval("${config.configurationMethod}()", context) as? Config ?: throw Exception("Configuration file must contain a config {} section at the end")

        // Use the config to build out object structure
        try {
            mongoClient = MongoClient(MongoClientURI(result.mongo.url))
            mongoClient.listDatabaseNames().first()
        } catch(ex: MongoException) {
            logger.error { "Failed to connect to MongoDB with masterURI [${result.mongo.url}, ${ex.message}"}
            throw SystemExitException("Failed to connect to MongoDB with masterURI [${result.mongo.url}, ${ex.message}", 2)
        }

        // Metric loggers
        val metricLoggers = mutableListOf<MetricLogger>()
        val simulations = mutableMapOf<String, Simulation>()

        // For all executors processes wait
        val workers = result.coordinator.tickers.map {
            when (it) {
                is RemoteConfig -> {
                    // Add simulation
                    simulations[it.simulation.javaClass.name] = it.simulation
                    // Create remote worker
                    RemoteWorker(it.name, config)
                }

                is LocalConfig -> {
                    simulations[it.simulation.javaClass.name] = it.simulation
                    // Metric logger
                    metricLoggers += LocalMetricLogger(it.name, metricsAggregator)
                    // Return the local worker facade
                    LocalWorkerFacade(it.name, mongoClient, when (it.loadPatternConfig) {
                        is ConstantConfig -> {
                            Constant(
                                ThreadedSimulationExecutor(it.simulation, metricLoggers.last(), it.name),
                                it.loadPatternConfig.numberOfCExecutions,
                                it.loadPatternConfig.executeEveryMilliseconds
                            )
                        }
                        else -> throw Exception("Unknown LoadPattern Config Object")
                    }, metricLoggers.last())
                }

                else -> throw Exception("Unknown Worker Config")
            }
        }

        // Register all the remote workers
        masterExecutorServer.nonInitializedWorkers = LinkedBlockingDeque(workers.filterIsInstance<RemoteWorker>())

        // Do we have the master flag enabled
        masterExecutorServer.start()

        // Wait for all workers to be ready
        workers.forEach {
            it.ready()
        }

        // Initialize all the simulations one
        simulations.values.forEach {
            logger.info { "calling init on simulation [${it.javaClass.simpleName}]" }
            it.init(mongoClient)
        }

        // Init workers if needed
        workers.forEach {
            it.init()
        }

        // Start the workers
        workers.forEach {
            it.start(result.coordinator.runForNumberOfTicks, result.coordinator.tickResolutionMiliseconds)
        }

        logger.info { "Stopping workers" }

        val stopThreads = workers.map {
            Thread(Runnable {
                it.stop()
            })
        }

        stopThreads.forEach { it.start() }

        logger.info { "Wait for workers to finish" }

        for (thread in stopThreads) {
            thread.join()
        }

        logger.info { "Starting generation of graph" }

        // Generate a graph
        GraphGenerator(
            simulations.values.first().javaClass.simpleName,
            config.graphOutputFilePath,
            config.graphOutputDPI,
            config.graphFilters,
            config.skipTicks
        ).generate(metricsAggregator)

        logger.info { "Finished executing simulation, terminating" }
    }

    companion object : KLogging()
}
