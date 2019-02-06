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
    fun start(client: MongoClient)
    fun execute(tick: Long, client: MongoClient) : Job
    fun stop(client: MongoClient)
}

data class SimulationOptions(val iterations: Int = 1000)

abstract class Simulation(val options: SimulationOptions = SimulationOptions()) {
    abstract fun mongodbConnection() : MongoClient
    abstract fun beforeAll(client: MongoClient)
    abstract fun before(client: MongoClient)
    abstract fun after(client: MongoClient)
    abstract fun init(client: MongoClient)
    abstract fun afterAll(client: MongoClient)

    fun execute(logger: MetricLogger = NoopLogger(), tick: Long) {
        val logEntry = logger.createLogEntry(this.javaClass.simpleName, tick)

        val time = measureNanoTime {
            run(logEntry)
        }

        logEntry.total = time
    }

    abstract fun run(logEntry: LogEntry = LogEntry("", 0))

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

    // Do any setup required for the full simulation
    override fun start(client: MongoClient) {
        logger.info { "[$name]: Executing beforeAll" }
        simulation.beforeAll(client)
        logger.info { "[$name]: beforeAll executed successfully" }
    }

    // Do any teardown required for the full simulation
    override fun stop(client: MongoClient) {
        logger.info { "[$name]: Executing stop" }
        simulation.afterAll(client)
        logger.info { "[$name]: stop executed successfully" }
    }

    override fun execute(tick: Long, client: MongoClient) : Job {
        return launch {
            simulation.before(client)
            simulation.execute(metricLogger, tick)
            simulation.after(client)
        }
    }

    companion object : KLogging()
}
