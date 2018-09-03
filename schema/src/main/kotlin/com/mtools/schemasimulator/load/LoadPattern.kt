package com.mtools.schemasimulator.load

import com.mongodb.MongoClient
import kotlinx.coroutines.experimental.Job

interface LoadPattern {
    fun init(client: MongoClient)
    fun start()
    fun tick(time: Long) : List<Job>
    fun stop()
}

/*
  MasterTicker
    -> WorkerTicker (Local/Distributed)
        -> tick
            -> load pattern
                -> simulator
            -> load pattern
                -> simulator
    -> WorkerTicker


 */
