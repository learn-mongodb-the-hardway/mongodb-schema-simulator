package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.cli.workers.LocalWorker
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.logger.NoopLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.jetbrains.kotlin.script.jsr223.KotlinJsr223JvmLocalScriptEngine
import java.net.URI
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager
import javax.script.SimpleScriptContext

data class MasterExecutorConfig (
    val master: Boolean,
    val uri: URI?,
    val config: String,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class MasterExecutor(private val config: MasterExecutorConfig) : Executor {
    private val masterExecutorServer = MasterExecutorServer(config)

    fun start() {
        var mongoClient: MongoClient?

        // Attempt to read the configuration Kotlin file
        val engine = ScriptEngineManager()
            .getEngineByExtension("kts")!! as? KotlinJsr223JvmLocalScriptEngine ?: throw SystemExitException("Expected the script engine to be a valid Kotlin Script engine", 2)

        // Read the string
        val configFileString = config.config

        // Create new context
        val context = SimpleScriptContext()

        // Load the Script
        engine.eval(configFileString, context)

        // Attempt to invoke simulation
        val result = engine.eval("configure()", context) as? Config ?: throw Exception("Configuration file must contain a config {} section at the end")

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
                    LocalWorker(it.name, mongoClient, when (it.loadPatternConfig) {
                        is ConstantConfig -> {
                            Constant(
                                ThreadedSimulationExecutor(it.simulation, metricLogger, it.name),
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
