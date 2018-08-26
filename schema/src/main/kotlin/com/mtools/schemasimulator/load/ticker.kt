package com.mtools.schemasimulator.load

/**
 * It's a synchronized load generator (a coordinator of action)
 * each tick will ping each load generator and let it decide what to apply of
 * load to the system
 **/
class MasterTicker(
    val slaveTickers: List<SlaveTicker> = listOf()
) {

//    val ticks: Long = 1000,
//    val tickInMiliseconds: Long = 100,
//    val loadPatterns: List<LoadPattern> = listOf<LoadPattern>()) {
//
//    // Internal variables
//    var currentTime = 0L
//    var running = false
//
//    // Thread that runs the load patterns
//    var thread: Thread = Thread(Runnable {
//        for (i in 0 until ticks step 1) {
//            // Delegate to loadPatterns
//            loadPatterns.forEach { it.execute(currentTime) }
//            // Tick the
//            Thread.sleep(tickInMiliseconds)
//            // Adjust the current time
//            currentTime += tickInMiliseconds
//        }
//    })
//
//    fun start() {
//        thread.start()
//        running = true
//    }
//
//    fun stop() {
//        running = false
//    }
}

interface SlaveTicker {
    fun tick(time: Long)
}

abstract class BaseSlaveTicker(val pattern: LoadPattern): SlaveTicker {
    abstract override fun tick(time: Long)
}

class LocalSlaveTicker(pattern: LoadPattern): BaseSlaveTicker(pattern)  {
    override fun tick(time: Long) {
        pattern.execute(time)
    }
}
