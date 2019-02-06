package com.mtools.schemasimulator.cli.workers

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.MasterExecutorConfig
import com.mtools.schemasimulator.logger.postMessage
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureErrorResponse
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.master.Start
import com.mtools.schemasimulator.messages.master.Stop
import mu.KLogging
import org.apache.http.client.utils.URIBuilder
import java.net.URI
import java.util.*
import java.io.InputStreamReader


class RemoteWorker(
    private val name: String,
    private val config: MasterExecutorConfig
): Worker {
    private var uri: URI? = null

    private val responseOk = """ok"\s*:\s*true""".toRegex()
    private var stopped = false

    override fun ready() {
        while(uri == null) {
            Thread.sleep(10)
        }
    }

    override fun init() {
        // Execute request
        val response = postMessage(uri!!, "/configure", Klaxon().toJsonString(Configure(name, String(Base64.getEncoder().encode(config.config.toByteArray())))))
        // Did we get a valid response
        if (response.statusLine.statusCode == 200) {
            val body = InputStreamReader(response.entity.content).readText()

            if (body.contains(responseOk)) {
                val responseObject = Klaxon().parse<ConfigureResponse>(body)
                println()
            } else {
                val errorResponse = Klaxon().parse<ConfigureErrorResponse>(body)
                println()
            }
        }
    }

    override fun start(numberOfTicks: Long, tickResolution: Long) {
        logger.debug { "start sent to $uri" }
        postMessage(uri!!, "/start", Klaxon().toJsonString(Start(numberOfTicks, tickResolution)))
    }

    override fun stop() {
        while (!stopped) {
            Thread.sleep(100)
        }
    }

    fun done() {
        stopped = true
    }

    fun assign(host: String, port: Int) {
        uri = URIBuilder()
            .setScheme("http")
            .setHost(host)
            .setPort(port)
            .build()
    }

    fun equalsHostPort(host: String, port: Int): Boolean {
        return uri!!.host == host && uri!!.port == port
    }

    companion object : KLogging()
}
