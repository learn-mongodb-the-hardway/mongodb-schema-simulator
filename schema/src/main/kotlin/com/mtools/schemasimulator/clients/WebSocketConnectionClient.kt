package com.mtools.schemasimulator.clients

import com.beust.klaxon.Klaxon
import com.xenomachina.argparser.SystemExitException
import mu.KLogging
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.lang.Exception
import java.net.URI

class WebSocketConnectionClient(
    private val uri: URI,
    private val maxReconnectAttempts: Int = 30,
    private val waitMSBetweenReconnectAttempts: Long = 1000,
    onOpen: (client: WebSocketConnectionClient) -> Unit,
    onMessage: (client: WebSocketConnectionClient, message: String?) -> Unit
) {
    private var currentReconnectAttempts = maxReconnectAttempts
    private var destroyed = false
    private var connected = false
    private var connecting = false

    // Handler for client
    private val onError:() -> Unit = fun() {
        connecting = false
        connected = false
    }

    // Client
    private val client = Client(this, uri, onMessage, onOpen, onError)

    private fun attemptReconnect(c: Client) {
        // Attempt to reconnect if the client failed (TODO silly simple way)
        while(true) {
            Thread.sleep(waitMSBetweenReconnectAttempts)

            // No more attempts left, throw an error
            if (currentReconnectAttempts == 0) {
                throw SystemExitException("Attempted to connect to [${uri.host}:${uri.port}] $maxReconnectAttempts times but failed", 2)
            }

            // We are connected already skip
            if (c.isOpen) {
                connected = true
                currentReconnectAttempts = maxReconnectAttempts
                break
            }

            // We are supposed to be connected but failed
            if (!connecting
                && !destroyed
                && c.isClosed) {
                currentReconnectAttempts -= 1
                c.reconnect()
            }
        }
    }

    fun connect() {
        connecting = true
        client.connect()

        // Check status of connection and reconnect if needed
        while(!destroyed) {
            attemptReconnect(client)
        }
    }

    fun send(obj: Any) {
        client.send(Klaxon().toJsonString(obj))
    }

    fun disconnect() {
        destroyed = true
        client.destroy()
    }

    private class Client(
        val client: WebSocketConnectionClient,
        uri: URI,
        val onMessage: (client: WebSocketConnectionClient, message: String?) -> Unit,
        val onOpen: (client: WebSocketConnectionClient) -> Unit,
        val onErrors: () -> Unit
    ): WebSocketClient(uri) {
        var destroyed = false

        fun destroy() {
            destroyed = true
            this.closeBlocking()
        }

        override fun onOpen(handshakedata: ServerHandshake?) {
            logger.info { "connection opened" }
            onOpen(client)
        }

        override fun onClose(code: Int, reason: String?, remote: Boolean) {
            logger.info { "connection closed from master: $code::$reason::$remote" }
            onErrors()
        }

        override fun onMessage(message: String?) {
            onMessage(client, message)
        }

        override fun onError(ex: Exception?) {
            logger.error { "connection error: $ex" }
            onErrors()
        }

        companion object : KLogging()
    }
}
