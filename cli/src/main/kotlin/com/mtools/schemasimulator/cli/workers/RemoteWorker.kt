package com.mtools.schemasimulator.cli.workers

import com.mtools.schemasimulator.cli.MasterExecutorConfig
import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.Stop
import com.mtools.schemasimulator.messages.master.Tick
import mu.KLogging
import java.net.URI
import java.util.*

class RemoteWorker(private val name: String, private val config: MasterExecutorConfig): Worker {
    private val configureReplyMessage = """method"\s*:\s*"configure"""".toRegex()
    private val stopReplyMessage = """method"\s*:\s*"stop"""".toRegex()
    private val metricsMessage = """method"\s*:\s*"metrics"""".toRegex()
    private var client: WebSocketConnectionClient? = null
    var initialized = false
    var stopped = false

    // Websocket onOpen method
    private val onOpen: (c: WebSocketConnectionClient) -> Unit = fun(c: WebSocketConnectionClient) {
        client = c
    }

    // Websocket onMessage
    private val onMessage: (client: WebSocketConnectionClient, message: String?) -> Unit = fun(_: WebSocketConnectionClient, message: String?) {
        logger.info { "onMessage [$message]" }

        // Process the replies
        if (message != null) {
            if (message.contains(configureReplyMessage)) {
                initialized = true
            } else if (message.contains(stopReplyMessage)) {
                stopped = true
            } else if (message.contains(metricsMessage)){
                println("== received metrics message")
            }
        }
    }

    override fun ready() {
        while(client == null) {
            Thread.sleep(10)
        }
    }

    override fun init() {
        // Fire the init message
        client!!.send(Configure(name, String(Base64.getEncoder().encode(config.config.toByteArray()))))

        // Wait for the response before we are done
        while (!initialized) {
            Thread.sleep(10)
        }
    }

    override fun tick(time: Long) {
        client!!.send(Tick(time))
    }

    override fun stop() {
        // Fire the init message
        client!!.send(Stop(name))

        // Wait for the response before we are done
        while (!stopped) {
            Thread.sleep(10)
        }

        // Shutdown the webclient
        client!!.disconnect()
    }

    fun assign(host: String, port: Int) {
        var localClient = WebSocketConnectionClient(
            URI.create("http://$host:$port"),
            config.maxReconnectAttempts,
            config.waitMSBetweenReconnectAttempts,
            onOpen,
            onMessage)

        // Start connection
        localClient.connect()
    }

    companion object : KLogging()
}
