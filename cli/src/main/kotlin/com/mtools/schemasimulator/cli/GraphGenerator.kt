package com.mtools.schemasimulator.cli

import com.mtools.schemasimulator.logger.MetricsAggregator
import org.knowm.xchart.BitmapEncoder
import org.knowm.xchart.XYChartBuilder
import org.knowm.xchart.XYSeries
import org.knowm.xchart.style.Styler
import org.knowm.xchart.style.markers.SeriesMarkers
import java.io.File
import java.text.DecimalFormat

class GraphGenerator(
    private val name:String,
    private val outputPath: File,
    private val dpi: Int,
    private val filters: List<String> = listOf(),
    private val skipTicks: Int = 0
) {
    private val df = DecimalFormat("#.00")

    fun generate(aggregator: MetricsAggregator) {
        // Go over all the keys
        val keys = aggregator.keys.sorted()

        // Data per field
        val dataByStep = mutableMapOf<String, Pair<MutableList<Long>, MutableList<Double>>>()

        // Generate series for each step
        keys.forEach { key ->
            if (key >= skipTicks) {
                aggregator.entries(key)
                    .entries
                    .filter { filters.isEmpty() || filters.contains(it.key) }
                    .forEach { entry ->

                        if (!dataByStep.containsKey(entry.key)) {
                            dataByStep[entry.key] = Pair(mutableListOf(), mutableListOf())
                        }

                        dataByStep[entry.key]!!.first.add(key)
                        dataByStep[entry.key]!!.second.add(if(entry.value.mean == 0.0) 1.0 else entry.value.mean / 1000000)
                    }
            }
        }

        // Get descriptive instance of special label
        val overallStats = aggregator.aggregate("total", skipTicks)
        val min = overallStats.min / 1000000
        val max = overallStats.max / 1000000
        val percentile95 = overallStats.getPercentile(95.0) / 1000000
        val percentile99 = overallStats.getPercentile(99.0) / 1000000
        val mean = overallStats.mean / 1000000

        // Create Chart
        val chart = XYChartBuilder()
            .width(1024)
            .height(768)
            .title("Mean execution Graph for: $name [min: $min ms, max: $max ms, mean: ${df.format(mean)}, %95: ${df.format(percentile95)} ms, %99: ${df.format(percentile99)} ms]")
            .xAxisTitle("Time (ms)")
            .yAxisTitle("Milliseconds").build()

        // Customize Chart
        chart.styler.legendPosition = Styler.LegendPosition.InsideNW
        chart.styler.defaultSeriesRenderStyle = XYSeries.XYSeriesRenderStyle.Area
        chart.styler.yAxisLabelAlignment = Styler.TextAlignment.Right
        chart.styler.yAxisDecimalPattern = "#,###.## ms"
        chart.styler.xAxisDecimalPattern = "#,###.## ms"
//        chart.styler.isYAxisLogarithmic = true
        chart.styler.plotMargin = 0
        chart.styler.plotContentSize = .95

        // Add series to graph
        dataByStep.forEach {
            val series = chart.addSeries(it.key, it.value.first, it.value.second)
            series.xySeriesRenderStyle = XYSeries.XYSeriesRenderStyle.Area
            series.marker = SeriesMarkers.NONE
        }

        // or save it in high-res
        BitmapEncoder.saveBitmapWithDPI(chart, outputPath.absolutePath, BitmapEncoder.BitmapFormat.PNG, dpi)
    }
}
