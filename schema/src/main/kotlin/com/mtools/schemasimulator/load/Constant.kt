package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import com.mtools.schemasimulator.executor.SimulationExecutor
import kotlinx.coroutines.experimental.Job
import mu.KLogging

class Constant(
    private val executor: SimulationExecutor,
    private val numberOfCExecutions: Long,
    private val executeEveryMilliseconds: Long
) : LoadPattern {
    var currentTime = 0L

    override fun init(client: MongoClient) {
        executor.init(client)
    }

    override fun start() {
        executor.start()
    }

    override fun stop() {
        executor.stop()
    }

    override fun tick(time: Long) : List<Job> {
        if ((time - currentTime) >= executeEveryMilliseconds) {
            logger.debug { "Executing Constant load pattern at $time" }
            val jobs = mutableListOf<Job>()

            for (i in 0 until numberOfCExecutions step 1) {
                jobs += executor.execute()
            }

            currentTime = time
            return jobs
        }

        return listOf()
    }

    companion object : KLogging()
}
