package com.mtools.schemasimulator.stats

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
//import org.apache.commons.math3.stat.descriptive.SummaryStatistics

class Statistics(window: Int? = null) {
//    private val summaryStatistics: SummaryStatistics = SummaryStatistics()
    private val descriptiveStatistics: DescriptiveStatistics = if (window != null) DescriptiveStatistics(window) else DescriptiveStatistics()

    fun addValue(value: Double) {
//        summaryStatistics.addValue(value)
        descriptiveStatistics.addValue(value)
    }

    val min: Double
//        get() = summaryStatistics.min
        get() = descriptiveStatistics.min

    val max: Double
//        get() = summaryStatistics.max
        get() = descriptiveStatistics.max

    val mean: Double
//        get() = summaryStatistics.mean
        get() = descriptiveStatistics.mean

    val values: List<Double>
        get() = descriptiveStatistics.values.toList()

    fun percentile(value: Double) : Double {
        return descriptiveStatistics.getPercentile(value)
    }
}
