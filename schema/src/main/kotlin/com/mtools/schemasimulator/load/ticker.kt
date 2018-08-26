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

    // Internal variables
    var currentTime = 0L
    var running = false

    // Thread that runs the load patterns
    var thread: Thread = Thread(Runnable {
        for (i in 0 until ticks step 1) {
            // Delegate to loadPatterns
            loadPatterns.forEach { it.execute(currentTime) }
            // Tick the
            Thread.sleep(tickInMiliseconds)
            // Adjust the current time
            currentTime += tickInMiliseconds
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
