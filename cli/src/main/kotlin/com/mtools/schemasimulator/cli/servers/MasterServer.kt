package com.mtools.schemasimulator.cli.servers

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.MasterExecutorConfig
import com.mtools.schemasimulator.clients.WebSocketConnectionClient
import com.mtools.schemasimulator.load.RemoteSlaveTicker
import com.mtools.schemasimulator.load.SlaveTicker
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.slave.CommandReply
import com.mtools.schemasimulator.messages.slave.Register
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.net.InetSocketAddress
import java.net.URI
import java.util.*
import java.util.concurrent.LinkedBlockingDeque

class MasterServer (
    private val config: MasterExecutorConfig,
    private val slaveTickers: LinkedBlockingDeque<SlaveTicker>
): WebSocketServer(InetSocketAddress(config.uri?.host ?: "127.0.0.1", config.uri!!.port)) {
    private val registerMessage = """method"\s*:\s*"register"""".toRegex()
    private val configureReplyMessage = """method"\s*:\s*"configure"""".toRegex()

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(registerMessage)) {
            val register = Klaxon().parse<Register>(message)

            // We have to have at least a slave ticker
            if (slaveTickers.size > 0 && register != null) {
                // Keeps the reference to the non initialized ticker
                var nonInitializedTicker: SlaveTicker? = null

                // Locate a ticker that is not ready
                for (i in 0 until slaveTickers.size) {
                    val ticker = slaveTickers.remove()

                    // Is it not ready, lets use this one
                    if (ticker is RemoteSlaveTicker && ticker.initialized().not()) {
                        nonInitializedTicker = ticker
                        break
                    } else {
                        slaveTickers.push(ticker)
                    }
                }

                // Send response message
                conn?.send(Klaxon().toJsonString(CommandReply(register.id, true)))

                // Create client
                if (nonInitializedTicker != null && nonInitializedTicker is RemoteSlaveTicker) {
                    // onOpen handler
                    val onOpen = fun(client: WebSocketConnectionClient) {
                        // Send the config message
                        client.send(Configure(
                            nonInitializedTicker.name,
                            String(Base64.getEncoder().encode(config.config.toByteArray()))))
                    }

                    // onMessage handler
                    val onMessage = fun(client: WebSocketConnectionClient, message: String?) {
                        if (message != null && message.contains(configureReplyMessage) && conn != null) {
                            val configure = Klaxon().parse<ConfigureResponse>(message)
                            if (configure != null) {
                                nonInitializedTicker.initialize(client)
                            }
                        }
                    }

                    // Create an instance of web socket client
                    val client = WebSocketConnectionClient(
                        URI.create("http://${register.host}:${register.port}"),
                        config.maxReconnectAttempts,
                        config.waitMSBetweenReconnectAttempts,
                        onOpen,
                        onMessage
                    )

                    // Attempt to connect
                    client.connect()
                }
            } else if (register != null) {
                conn?.send(Klaxon().toJsonString(CommandReply(register.id, false)))
            }
        }
    }

    override fun onStart() {
        logger.info { "starting server on: ${config.uri?.host ?: "localhost"}:${config.uri!!.port}" }
    }

    override fun onError(conn: WebSocket?, ex: java.lang.Exception?) {
    }

    companion object : KLogging()
}
