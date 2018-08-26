package com.mtools.schemasimulator.load

interface LoadPattern {
    fun execute(time: Long)
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
