package com.mtools.schemasimulator.executor

import com.mongodb.MongoClient
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.logger.NoopLogger
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import kotlin.system.measureNanoTime

interface SimulationExecutor {
    fun init(client: MongoClient)
    fun start()
    fun execute() : Job
    fun stop()
}

data class SimulationOptions(val iterations: Int = 1000)

abstract class Simulation(val options: SimulationOptions = SimulationOptions()) {
    abstract fun mongodbConnection() : MongoClient
    abstract fun beforeAll()
    abstract fun before()
    abstract fun after()
    abstract fun init(client: MongoClient)

    fun execute(logger: MetricLogger = NoopLogger()) {
        val logEntry = logger.createLogEntry(this.javaClass.simpleName)

        val time = measureNanoTime {
            run(logEntry)
        }

        logEntry.total = time
    }

    abstract fun run(logEntry: LogEntry = LogEntry(""))
    abstract fun afterAll()
}

class ThreadedSimulationExecutor(
    private val simulation: Simulation,
    private val metricLogger: MetricLogger = NoopLogger()) : SimulationExecutor {

    override fun init(client: MongoClient) {
        simulation.init(client)
    }

    // Do any setup required for the full simulation
    override fun start() {
        simulation.beforeAll()
    }

    // Do any teardown required for the full simulation
    override fun stop() {
        simulation.afterAll()
    }

    override fun execute() : Job {
        return launch {
            simulation.before()
            simulation.execute(metricLogger)
            simulation.after()
        }
    }
}
