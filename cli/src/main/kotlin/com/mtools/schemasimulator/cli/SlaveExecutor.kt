package com.mtools.schemasimulator.cli

import com.mtools.schemasimulator.messages.slave.Register
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.handshake.ServerHandshake
import org.java_websocket.server.WebSocketServer
import java.lang.Exception
import java.net.InetSocketAddress
import java.net.URI

data class SlaveExecutorConfig(
    val masterHost: String,
    val masterPort: Int,
    val host: String,
    val port: Int,
    val maxReconnectAttempts: Int = 30,
    val waitMSBetweenReconnectAttempts: Long = 1000)

class SlaveExecutor(val config: SlaveExecutorConfig) : Executor {
    private var currentReconnectAttempts = config.maxReconnectAttempts

    fun start() {
        // We are going to set up our services on the provided host and port
        val server = SlaveSocketServer(config)
        server.start()

        // Let's attempt to signal that we are available
        val client = SlaveToMasterClient(URI.create("http://${config.masterHost}:${config.masterPort}"), config)
        client.connect()

        // Attempt to reconnect if the client failed (TODO silly simple way)
        while(true) {
            if (currentReconnectAttempts == 0) {
                throw SystemExitException("Attempted to connect to [${config.masterHost}:${config.masterPort}] ${config.maxReconnectAttempts} times but failed", 2)
            }

            if (client.isClosed) {
                currentReconnectAttempts -= 1
                client.reconnect()
            } else if (client.isOpen) {
                currentReconnectAttempts = config.maxReconnectAttempts
            }

            Thread.sleep(config.waitMSBetweenReconnectAttempts)
        }

        // Shut down server
        client.closeBlocking()
        server.stop()
    }
}

private class SlaveSocketServer(val config: SlaveExecutorConfig): WebSocketServer(InetSocketAddress(config.host, config.port)) {

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }
    }

    override fun onStart() {
        logger.info { "starting server on: ${config.host}:${config.port}" }
    }

    override fun onError(conn: WebSocket?, ex: Exception?) {
    }

    companion object : KLogging()
}

private class SlaveToMasterClient(uri: URI, val config: SlaveExecutorConfig): WebSocketClient(uri) {
    override fun onOpen(handshakedata: ServerHandshake?) {
        logger.info { "connection opened" }

        // Register with the master
        this.send(com.beust.klaxon.Klaxon().toJsonString(Register(config.host, config.port)))
    }

    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from master: $code::$reason::$remote" }
    }

    override fun onMessage(message: String?) {
    }

    override fun onError(ex: Exception?) {
        logger.error { "connection error: $ex" }
    }

    companion object : KLogging()
}
