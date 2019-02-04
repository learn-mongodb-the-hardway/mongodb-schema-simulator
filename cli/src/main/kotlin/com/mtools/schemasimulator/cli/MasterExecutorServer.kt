package com.mtools.schemasimulator.cli

import com.beust.klaxon.Klaxon
import com.mtools.schemasimulator.cli.workers.RemoteWorker
import com.mtools.schemasimulator.messages.worker.Register
import mu.KLogging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingDeque

class MasterExecutorServer(config: MasterExecutorConfig): WebSocketServer(InetSocketAddress(config.uri?.host ?: "127.0.0.1", config.uri!!.port)) {
    private val registerMessage = """method"\s*:\s*"register"""".toRegex()
    lateinit var nonInitializedWorkers: LinkedBlockingDeque<RemoteWorker>
    var workers = LinkedBlockingDeque<RemoteWorker>()

    override fun onOpen(conn: WebSocket?, handshake: ClientHandshake?) {
        logger.info { "connection from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onClose(conn: WebSocket?, code: Int, reason: String?, remote: Boolean) {
        logger.info { "connection closed from client: ${conn!!.remoteSocketAddress.address.hostAddress}" }
    }

    override fun onMessage(conn: WebSocket?, message: String?) {
        logger.info { "onMessage from [${conn!!.remoteSocketAddress.address.hostAddress}]: [$message]" }

        if (message != null && message.contains(registerMessage) && conn != null) {
            val register = Klaxon().parse<Register>(message)

            // Attempt to grab the first worker
            val worker = nonInitializedWorkers.takeFirst()

            // Register worker ready for work
            if (register != null && worker != null) {
                worker.assign(register.host, register.port)
                workers.add(worker)
            }
        }
    }

    override fun onStart() {
    }

    override fun onError(conn: WebSocket?, ex: java.lang.Exception?) {
    }

    companion object : KLogging()
}
