package com.mtools.schemasimulator.cli

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoException
import com.mtools.schemasimulator.cli.config.Config
import com.mtools.schemasimulator.cli.config.ConstantConfig
import com.mtools.schemasimulator.cli.config.LocalConfig
import com.mtools.schemasimulator.cli.config.RemoteConfig
import com.mtools.schemasimulator.cli.workers.LocalWorker
import com.mtools.schemasimulator.cli.workers.MetricsAggregator
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.executor.ThreadedSimulationExecutor
import com.mtools.schemasimulator.load.Constant
import com.mtools.schemasimulator.logger.LocalMetricLogger
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.jetbrains.kotlin.script.jsr223.KotlinJsr223JvmLocalScriptEngine
import java.net.URI
import java.util.concurrent.LinkedBlockingDeque
import javax.script.ScriptEngineManager
import javax.script.SimpleScriptContext
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.BitmapEncoder
import org.knowm.xchart.XYChartBuilder
import org.knowm.xchart.XYSeries
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.style.Styler
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler.LegendPosition

data class MasterExecutorConfig (
    val master: Boolean,
    val uri: URI?,
    val config: String,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

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
        val result = engine.eval("configure()", context) as? Config ?: throw Exception("Configuration file must contain a config {} section at the end")

        // Use the config to build out object structure
        try {
            mongoClient = MongoClient(MongoClientURI(result.mongo.url))
            mongoClient.listDatabaseNames().first()
        } catch(ex: MongoException) {
            logger.error { "Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}"}
            throw SystemExitException("Failed to connect to MongoDB with uri [${result.mongo.url}, ${ex.message}", 2)
        }

        // Metric logger
        val metricLogger = LocalMetricLogger()

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

        // Register all the remote workers
        masterExecutorServer.nonInitializedWorkers = LinkedBlockingDeque(workers.filterIsInstance<RemoteWorker>())

        // Do we have the master flag enabled
        masterExecutorServer.start()

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

        // Merge all the data
        metricsAggregator.processTicks(metricLogger.logEntries)

        logger.info { "Starting generation of graph" }

        // Generate a graph
        GraphGenerator().generate(metricsAggregator.metrics)

        logger.info { "Finished executing simulation, terminating" }
    }

    companion object : KLogging()
}

class GraphGenerator() {
    fun generate(entries: MutableMap<Long, MutableMap<String, SummaryStatistics>>) {
        // Go over all the keys
        val keys = entries.keys.sorted()
        // Double Arrays
        val xDoubles = mutableListOf<Long>()
        val yDoubles = mutableListOf<Double>()

        // Calculate the total series
        keys.forEach {key ->
            xDoubles.add(key)
            yDoubles.add(entries[key]!!["total"]!!.mean)
        }

        // Create Chart
        val chart = XYChartBuilder()
            .width(1024)
            .height(768)
            .title("Execution Graph")
            .xAxisTitle("Time (ms)")
            .yAxisTitle("Milliseconds").build()

        // Customize Chart
        chart.styler.legendPosition = LegendPosition.InsideNW
        chart.styler.defaultSeriesRenderStyle = XYSeriesRenderStyle.Area
        chart.styler.yAxisLabelAlignment = Styler.TextAlignment.Right
        chart.styler.yAxisDecimalPattern = "#,###.## ms"
        chart.styler.isYAxisLogarithmic = true;
        chart.styler.plotMargin = 0
        chart.styler.plotContentSize = .95

        // Data per field
        val dataByStep = mutableMapOf<String, Pair<MutableList<Long>, MutableList<Double>>>()

        // Generate series for each step
        keys.forEach { key ->
            entries[key]!!.entries.forEach {
                if (!dataByStep.containsKey(it.key)) {
                    dataByStep[it.key] = Pair(mutableListOf(), mutableListOf())
                }

                dataByStep[it.key]!!.first.add(key)
                dataByStep[it.key]!!.second.add(it.value.mean)
            }
        }

        // Add series to graph
        dataByStep.forEach {
            val series = chart.addSeries(it.key, it.value.first, it.value.second)
            series.xySeriesRenderStyle = XYSeries.XYSeriesRenderStyle.Area
            series.marker = SeriesMarkers.NONE
        }

        // Save it
        BitmapEncoder.saveBitmap(chart, "./Sample_Chart", BitmapFormat.PNG)

        // or save it in high-res
        BitmapEncoder.saveBitmapWithDPI(chart, "./Sample_Chart_300_DPI", BitmapFormat.PNG, 300)
    }
}
