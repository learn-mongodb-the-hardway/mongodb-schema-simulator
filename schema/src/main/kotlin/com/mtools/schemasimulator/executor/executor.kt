package com.mtools.schemasimulator.executor

import com.mongodb.MongoClient
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.logger.NoopLogger
import com.mtools.schemasimulator.schemas.Scenario
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import mu.KLogging
import kotlin.system.measureNanoTime

interface SimulationExecutor {
    fun init(client: MongoClient)
    fun start()
    fun execute(tick: Long) : Job
    fun stop()
}

data class SimulationOptions(val iterations: Int = 1000)

abstract class Simulation(val options: SimulationOptions = SimulationOptions()) {
    abstract fun mongodbConnection() : MongoClient
    abstract fun beforeAll()
    abstract fun before()
    abstract fun after()
    abstract fun init(client: MongoClient)

    fun execute(logger: MetricLogger = NoopLogger(), tick: Long) {
        val logEntry = logger.createLogEntry(this.javaClass.simpleName, tick)

        val time = measureNanoTime {
            run(logEntry)
        }

        logEntry.total = time
    }

    abstract fun run(logEntry: LogEntry = LogEntry("", 0))
    abstract fun afterAll()

    fun createIndexes(indexCreator: Scenario) {
        // Create all relevant indexes
        indexCreator.indexes().forEach {
            mongodbConnection()
                .getDatabase(it.db)
                .getCollection(it.collection)
                .createIndex(it.keys, it.options)
        }
    }
}

class ThreadedSimulationExecutor(
    private val simulation: Simulation,
    private val metricLogger: MetricLogger = NoopLogger(),
    private val name: String) : SimulationExecutor {

    override fun init(client: MongoClient) {
        logger.info { "[$name]: Executing init" }
        simulation.init(client)
        logger.info { "[$name]: init executed successfully" }
    }

    // Do any setup required for the full simulation
    override fun start() {
        logger.info { "[$name]: Executing beforeAll" }
        simulation.beforeAll()
        logger.info { "[$name]: beforeAll executed successfully" }
    }

    // Do any teardown required for the full simulation
    override fun stop() {
        logger.info { "[$name]: Executing stop" }
        simulation.afterAll()
        logger.info { "[$name]: stop executed successfully" }
    }

    override fun execute(tick: Long) : Job {
        return launch {
            simulation.before()
            simulation.execute(metricLogger, tick)
            simulation.after()
        }
    }

    companion object : KLogging()
}
