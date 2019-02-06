package com.mtools.schemasimulator.cli.workers

interface Worker {
    fun ready()
    fun init()
    fun stop()
    fun start(numberOfTicks: Long, tickResolution: Long)
}
