package com.mtools.schemasimulator.cli

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.workers.MetricsAggregator
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.messages.master.Done
import com.mtools.schemasimulator.messages.worker.MetricsResult
import com.mtools.schemasimulator.messages.worker.Register
import mu.KLogging
import spark.kotlin.Http
import spark.kotlin.RouteHandler
import spark.kotlin.ignite
import java.io.StringReader
import java.util.concurrent.LinkedBlockingDeque

class MasterExecutorServer(
    private val config: MasterExecutorConfig,
    private val metricsAggregator: MetricsAggregator
) {
    lateinit var nonInitializedWorkers: LinkedBlockingDeque<RemoteWorker>
    var workers = LinkedBlockingDeque<RemoteWorker>()

    fun start() {
        val http: Http = ignite()
            .port(config.uri!!.port)

        http.post("/register") {
            logger.info("/register ${config.uri}")
            // Attempt to grab the first worker
            val worker = nonInitializedWorkers.takeFirst()

            // Register worker ready for work
            if (worker != null) {
                val register = Klaxon().parse<Register>(this.request.body())
                worker.assign(register!!.host, register.port)
                workers.add(worker)
            }

            true
        }

        http.post("/metrics") {
            handleMetrics(this)
        }

        http.post("/worker/done") {
            handleDone(this)
        }

        http.service.awaitInitialization()
    }

    private fun handleMetrics(routeHandler: RouteHandler): Any {
        val body = routeHandler.request.body()
        body ?: return ""

        // Get the json object
        val metrics = Klaxon().parseJsonObject(StringReader(body))
        // Get the ticks
        val ticks = metrics.array<JsonObject>("ticks")!!

        logger.info("metrics received [${metrics.string("host")}:${metrics.int("port")}]:[Max Tick:${ticks.map {
            it.int("tick")!!
        }.max()}]")

        // Process the ticks
        metricsAggregator.processTicks(ticks)

        return ""
    }

    private fun handleDone(routeHandler: RouteHandler): Any {
        val body = routeHandler.request.body()
        body ?: return ""

        val done = Klaxon().parse<Done>(body)
        done ?: return ""

        logger.info("/worker done [${done.host}:${done.port}]")

        // Locate the worker and signal done
        workers.forEach { worker ->
            if (worker.equalsHostPort(done.host, done.port)) {
                worker.done()
            }
        }

        return body
    }

    companion object : KLogging()
}
