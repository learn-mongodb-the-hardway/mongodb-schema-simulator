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

    override fun start(client: MongoClient) {
        executor.start(client)
    }

    override fun stop(client: MongoClient) {
        executor.stop(client)
    }

    override fun tick(time: Long, client: MongoClient) : List<Job> {
        if ((time - currentTime) >= executeEveryMilliseconds) {
            logger.debug { "Executing Constant load pattern at $time" }
            val jobs = mutableListOf<Job>()

            for (i in 0 until numberOfCExecutions step 1) {
                jobs += executor.execute(time, client)
            }

            currentTime = time
            return jobs
        }

        return listOf()
    }

    companion object : KLogging()
}
