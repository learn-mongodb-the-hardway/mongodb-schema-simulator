package com.mtools.schemasimulator.cli

import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.knowm.xchart.BitmapEncoder
import org.knowm.xchart.XYChartBuilder
import org.knowm.xchart.XYSeries
import org.knowm.xchart.style.Styler
import org.knowm.xchart.style.markers.SeriesMarkers
import java.io.File

class GraphGenerator(private val name:String, private val outputPath: File, private val dpi: Int, private val filters: List<String> = listOf()) {
    fun generate(entries: Map<Long, Map<String, SummaryStatistics>>) {
        // Go over all the keys
        val keys = entries.keys.sorted()
        // Double Arrays
        val xDoubles = mutableListOf<Long>()
        val yDoubles = mutableListOf<Double>()
        var max: Double = Double.MIN_VALUE
        var min: Double = Double.MAX_VALUE

        // Calculate the total series
        keys.forEach {key ->
            val entry = entries.getValue(key).getValue("total")
            if (entry.max > max) max = entry.max
            if (entry.min < min && entry.min != 0.0) min = entry.min
            xDoubles.add(key)
            yDoubles.add(entry.mean)
        }

        // Create Chart
        val chart = XYChartBuilder()
            .width(1024)
            .height(768)
            .title("Execution Graph for: $name [min: $min ms, max: $max ms]")
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

        // Data per field
        val dataByStep = mutableMapOf<String, Pair<MutableList<Long>, MutableList<Double>>>()

        // Generate series for each step
        keys.forEach { key ->
            entries[key]!!
                .entries
                .filter { filters.isEmpty() || filters.contains(it.key) }
                .forEach {

                if (!dataByStep.containsKey(it.key)) {
                    dataByStep[it.key] = Pair(mutableListOf(), mutableListOf())
                }

                dataByStep[it.key]!!.first.add(key)
                dataByStep[it.key]!!.second.add(if(it.value.mean == 0.0) 1.0 else it.value.mean)
            }
        }

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
