package com.mtools.schemasimulator.load

import com.mtools.schemasimulator.engine.SimulationExecutor
import kotlinx.coroutines.experimental.Job
import mu.KLogging

class Constant(
    private val executor: SimulationExecutor,
    private val numberOfCExecutions: Long,
    private val executeEveryMilliseconds: Long
) : LoadPattern {
    var currentTime = 0L

    override fun start() {
        executor.start()
    }

    override fun stop() {
        executor.stop()
    }

    override fun execute(time: Long) : List<Job> {
        if ((time - currentTime) >= executeEveryMilliseconds) {
            logger.info { "Executing Constant load pattern at $time" }
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
