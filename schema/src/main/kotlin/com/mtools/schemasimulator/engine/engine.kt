package com.mtools.schemasimulator.engine

import com.mongodb.MongoClient

interface Engine {
    fun execute(simulation: Simulation)
}

data class SimulationOptions(val iterations: Int = 1000)

abstract class Simulation(val options: SimulationOptions = SimulationOptions()) {
    abstract fun mongodbConnection() : MongoClient
    abstract fun beforeAll()
    abstract fun before()
    abstract fun after()
    abstract fun execute()
    abstract fun afterAll()
}

class SingleThreadedEngine: Engine {
    override fun execute(simulation: Simulation) {
        // We have to set execute the before All
        simulation.beforeAll()

        // Execute the simulation for the number of iterations indicated
        for (i in 0 until simulation.options.iterations) {
            simulation.execute()
        }

        // Tear down everything
        simulation.afterAll()
    }
}

class MultiThreadedEngine: Engine {
    override fun execute(simulation: Simulation) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
