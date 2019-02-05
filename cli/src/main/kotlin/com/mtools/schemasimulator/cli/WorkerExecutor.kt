package com.mtools.schemasimulator.cli

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.servers.WorkerServer
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.messages.worker.Register
import mu.KLogging
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import java.net.URI

data class WorkerExecutorConfig(
    val masterURI: URI,
    val uri: URI,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class WorkerExecutor(private val config: WorkerExecutorConfig) : Executor {
    fun start() {
        // We are going to set up our services on the provided host and port
        val server = WorkerServer(config)
        // Start the server`
        server.start()

        Thread {
            Thread.sleep(5000)
            // Send the register message
            val httpclient = HttpClients.createDefault()
            val uri = URIBuilder()
                .setScheme("http")
                .setHost(config.masterURI.host)
                .setPort(config.masterURI.port)
                .setPath("/register")
                .build()

            RemoteWorker.logger.info { "get to $uri"}
            // Post
            val post = HttpPost(uri)
            post.entity = StringEntity(Klaxon().toJsonString(Register(config.uri.host, config.uri.port)))
            // Execute request
            httpclient.execute(post)
        }.start()
    }

    companion object : KLogging()
}
