package com.mtools.schemasimulator.stats

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

class Statistics(window: Int? = null) {
    private val descriptiveStatistics: DescriptiveStatistics = if (window != null) DescriptiveStatistics(window) else DescriptiveStatistics()

    fun addValue(value: Double) {
        descriptiveStatistics.addValue(value)
    }

    val min: Double
        get() = descriptiveStatistics.min

    val max: Double
        get() = descriptiveStatistics.max

    val mean: Double
        get() = descriptiveStatistics.mean

    val values: List<Double>
        get() = descriptiveStatistics.values.toList()

    fun percentile(value: Double) : Double {
        return descriptiveStatistics.getPercentile(value)
    }
}
