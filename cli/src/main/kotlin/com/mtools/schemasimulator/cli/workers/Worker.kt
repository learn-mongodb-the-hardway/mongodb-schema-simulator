package com.mtools.schemasimulator.cli.workers

interface Worker {
    fun ready()
    fun init()
    fun tick(time: Long)
    fun stop()
}
