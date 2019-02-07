package com.mtools.schemasimulator.stats

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.SummaryStatistics

class Statistics(window: Int? = null) {
    private val summaryStatistics: SummaryStatistics = SummaryStatistics()
    val descriptiveStatistics: DescriptiveStatistics = if (window != null) DescriptiveStatistics(window) else DescriptiveStatistics()

    fun addValue(value: Double) {
        summaryStatistics.addValue(value)
        descriptiveStatistics.addValue(value)
    }

    val min: Double
        get() = summaryStatistics.min

    val max: Double
        get() = summaryStatistics.max

    val mean: Double
        get() = summaryStatistics.mean

    fun percentile(value: Double) : Double {
        return descriptiveStatistics.getPercentile(value)
    }
}
