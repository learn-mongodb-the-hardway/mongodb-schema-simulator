package com.mtools.schemasimulator.cli

import javax.script.ScriptEngineManager

class Executor(config: String) {
    fun execute() {
        val engine = ScriptEngineManager().getEngineByExtension("kts")!!
        engine.eval("val x = 3")
        println(engine.eval("x + 2"))  // Prints out 5
    }
}
