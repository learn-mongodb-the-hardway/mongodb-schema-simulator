package com.mtools.schemasimulator.cli

import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.cli.servers.WorkerServer
import com.mtools.schemasimulator.messages.worker.Register
import kotlinx.coroutines.experimental.launch
import mu.KLogging
import java.net.URI

data class WorkerExecutorConfig(
    val masterURI: URI,
    val uri: URI,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class WorkerExecutor(private val config: WorkerExecutorConfig) : Executor {
    lateinit var client: WebSocketConnectionClient

    fun start() {
        // On open function
        val onOpen = fun(client: WebSocketConnectionClient) {
            logger.info { "Worker received onOpen event [${config.uri.host}:${config.uri.port}]" }
            client.send(Register(config.uri.host, config.uri.port))
        }

        // Let's attempt to signal that we are available
        client = WebSocketConnectionClient(
            config.masterURI,
            config.maxReconnectAttempts,
            config.waitMSBetweenReconnectAttempts,
            onOpen) { _, message ->
            logger.debug ("Worker received message: [$message]")
        }

        // Shutdown handler
        val onShutdown = fun(s: WorkerServer) {
            logger.info { "Worker shutting down" }

            launch {
                s.stop()
                client.disconnect()
            }
        }

        // We are going to set up our services on the provided host and port
        val server = WorkerServer(config, onShutdown)
        // Start the server`
        server.start()
        // Start the Websocket client connection
        client.connect()
    }

    companion object : KLogging()
}
