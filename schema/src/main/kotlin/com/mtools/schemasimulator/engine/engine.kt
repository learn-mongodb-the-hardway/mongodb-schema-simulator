package com.mtools.schemasimulator.engine

import com.mongodb.MongoClient
import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.logger.MetricLogger
import com.mtools.schemasimulator.logger.NoopLogger
import kotlin.system.measureNanoTime

interface Engine {
    fun execute(simulations: List<Simulation>)
}

data class SimulationOptions(val iterations: Int = 1000)

abstract class Simulation(val options: SimulationOptions = SimulationOptions()) {
    abstract fun mongodbConnection() : MongoClient
    abstract fun beforeAll()
    abstract fun before()
    abstract fun after()

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

class SingleThreadedEngine(private val metricLogger: MetricLogger = NoopLogger()) : Engine {
    override fun execute(simulations: List<Simulation>) {
        // We have to set execute the before All
        simulations.forEach { it.beforeAll() }

        // Execute the simulations in parallel
        val threads = simulations.map {
            Thread(Runnable {
                for (i in 0 until it.options.iterations) {
                    it.before()
                    it.execute(metricLogger)
                    it.after()
                }
            })
        }

        // Start all the threads
        threads.forEach { it.start() }

        // Wait for threads to finish
        threads.forEach { it.join() }

        // Tear down everything
        simulations.forEach { it.afterAll() }
    }
}

//class MultiThreadedEngine: Engine {
//    override fun execute(simulation: Simulation) {
//        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//    }
//}
