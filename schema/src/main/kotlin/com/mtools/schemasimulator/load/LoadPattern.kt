package com.mtools.schemasimulator.load

import kotlinx.coroutines.experimental.Job

interface LoadPattern {
    fun start()
    fun execute(time: Long) : List<Job>
    fun stop()
}

/*
  MasterTicker
    -> SlavedTicker (Local/Distributed)
        -> tick
            -> load pattern
                -> simulator
            -> load pattern
                -> simulator
    -> SlavedTicker


 */
