package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.cli.servers.MasterServer
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LocalSlaveTicker
import com.mtools.schemasimulator.load.MasterTicker
import com.mtools.schemasimulator.load.RemoteSlaveTicker
import com.mtools.schemasimulator.load.SlaveTicker
import com.mtools.schemasimulator.logger.NoopLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import java.net.URI
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager

data class MasterExecutorConfig (
    val master: Boolean,
    val uri: URI?,
    val config: String,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class MasterExecutor(private val config: MasterExecutorConfig) : Executor {
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

        // Build the slave tickers
        val tickers = mapTickers(result, mongoClient, metricLogger)

        // Do we have the master flag enabled
        if (config.master) {
            val server = MasterServer(config, LinkedBlockingDeque(tickers.toList()))
            server.start()
        }

        // Wait for all slave tickers to be armed
        while (true) {
            val ready = tickers.map {
                it.ready()
            }.reduce { acc, value -> acc.and(value) }
            // If all tickers ready break
            if (ready) break
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

    private fun mapTickers(result: Config, mongoClient: MongoClient, metricLogger: NoopLogger): List<SlaveTicker> {
        return result.coordinator.tickers.map {
            when (it) {
                is LocalConfig -> {
                    val loadPatternConfig = it.loadPatternConfig

                    LocalSlaveTicker(it.name, mongoClient, when (loadPatternConfig) {
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
    }

    companion object : KLogging()
}
