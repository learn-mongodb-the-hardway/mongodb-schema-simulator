package com.mtools.schemasimulator.load

import com.mtools.schemasimulator.engine.Engine

class Constant(val engine: Engine,
               val executions: Long) : LoadPattern {
    override fun execute(time: Long) {
        engine.execute()
    }
}
