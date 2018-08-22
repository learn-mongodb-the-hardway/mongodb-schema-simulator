package com.mtools.schemasimulator.engine

import org.junit.jupiter.api.Test
import javax.script.ScriptEngineManager

class SingleThreadedEngineTest {

    @Test
    fun initializeEngine() {
        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
        engine.eval("val x = 3")
        println(engine.eval("x + 2"))  // Prints out 5
    }
}
