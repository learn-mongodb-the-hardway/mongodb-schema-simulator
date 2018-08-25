package com.mtools.schemasimulator.load

/**
 * It's a synchronized load generator (a coordinator of action)
 * each tick will ping each load generator and let it decide what to apply of
 * load to the system
 **/
class Ticker(
    val ticks: Long = 1000,
    val tickInMiliseconds: Long = 100,
    val loadPatterns: List<LoadPattern> = listOf<LoadPattern>()) {
    var running = false
    var thread: Thread = Thread(Runnable {
        for (i in 0 until ticks step 1) {
            Thread.sleep(tickInMiliseconds)
        }
    })

    fun start() {
        thread.start()
        running = true
    }

    fun stop() {
        running = false
    }
}
