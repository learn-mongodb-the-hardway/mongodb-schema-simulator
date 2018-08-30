package com.mtools.schemasimulator.cli

import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.cli.servers.SlaveServer
import com.mtools.schemasimulator.messages.slave.Register
import mu.KLogging
import java.net.URI

data class SlaveExecutorConfig(
    val masterURI: URI,
    val uri: URI,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class SlaveExecutor(private val config: SlaveExecutorConfig) : Executor {
    lateinit var client: WebSocketConnectionClient

    fun start() {
        // We are going to set up our services on the provided host and port
        val server = SlaveServer(config)
        server.start()

        // On open function
        val onOpen = fun(client: WebSocketConnectionClient) {
            logger.info { "Slave received onOpen event" }
            client.send(Register(config.uri.host, config.uri.port))
        }

        // Let's attempt to signal that we are available
        client = WebSocketConnectionClient(
            config.masterURI,
            config.maxReconnectAttempts,
            config.waitMSBetweenReconnectAttempts,
            onOpen) { client, message ->
            logger.info("Slave received message: [$message]")
        }

        client.connect()
    }

    companion object : KLogging()
}
