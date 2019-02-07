package com.mtools.schemasimulator.stats

import org.junit.jupiter.api.Test

class StatisticsTest {

    @Test
    fun simpleTest() {
        val stats = Statistics()
        for (value in 0..1000000) {
            stats.addValue(value.toDouble())
        }

        println("min = ${stats.min}")
        println("max = ${stats.max}")
        println("mean = ${stats.mean}")
        println("percentile95 = ${stats.percentile(95.0)}")
        println("percentile99 = ${stats.percentile(99.0)}")
    }
}
