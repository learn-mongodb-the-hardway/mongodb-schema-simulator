package com.mtools.schemasimulator.cli.servers

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.SlaveExecutorConfig
import com.mtools.schemasimulator.messages.master.Configure
import com.mtools.schemasimulator.messages.master.ConfigureResponse
import com.mtools.schemasimulator.messages.master.Tick
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.lang.Exception
import java.net.InetSocketAddress
import javax.script.ScriptEngineManager

class SlaveServer(val config: SlaveExecutorConfig): WebSocketServer(InetSocketAddress(config.uri.host, config.uri.port)) {
    private val configureMessage = """method"\s*:\s*"configure"""".toRegex()
    private val tickMessage = """method"\s*:\s*"tick"""".toRegex()

    // Attempt to read the configuration Kotlin file
    val engine = ScriptEngineManager().getEngineByExtension("kts")!!

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(configureMessage) && conn != null) {
            val configure = Klaxon().parse<Configure>(message)
            if (configure != null) {
                logger.info { "received config message, setup slave executor" }

                // Load the scenario
                val result = engine.eval(configure.configString)

                // Send a response
                conn.send(Klaxon().toJsonString(ConfigureResponse(configure.id)))
            }
        } else if (message != null && message.contains(tickMessage) && conn != null) {
            val tick = Klaxon().parse<Tick>(message)
            if (tick != null) {
                logger.info { "received tick message: ${tick.time}" }
            }
        }
    }

    override fun onStart() {
        logger.info { "starting server on: ${config.uri.host}:${config.uri.port}" }
    }

    override fun onError(conn: WebSocket?, ex: Exception?) {
    }

    companion object : KLogging()
}
