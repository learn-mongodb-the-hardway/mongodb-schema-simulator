package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.executor.SimulationExecutor
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.load.LocalSlaveTicker
import com.mtools.schemasimulator.load.MasterTicker
import com.mtools.schemasimulator.logger.NoopLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import java.io.Reader
import javax.script.ScriptEngineManager

class Executor(private val config: Reader, master: Boolean) {
    fun execute() {
        var mongoClient: MongoClient?
        // Attempt to read the configuration Kotlin file
        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
        // Load the file
        val result = engine.eval(config)

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
        val slaveTickers = result.coordinator.tickers.map {
            when (it) {
                is LocalConfig -> {
                    val loadPatternConfig = it.loadPatternConfig

                    LocalSlaveTicker(mongoClient, when(loadPatternConfig) {
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
                else -> throw Exception("Unknown Load Generator Config")
//                is RemoteConfig -> {
//
//                }
            }
        }

        // Build the structure
        val ticker = MasterTicker(
            slaveTickers,
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
