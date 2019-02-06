package com.mtools.schemasimulator.messages.master

import com.beust.klaxon.Json
import com.mtools.schemasimulator.messages.MethodCall
import com.mtools.schemasimulator.messages.MethodErrorResponse
import com.mtools.schemasimulator.messages.MethodResponse
import java.util.*

data class Configure(val name: String, val config: String) : MethodCall("configure") {
    @Json(ignored = true)
    val configString: String
        get() {
            return String(Base64.getDecoder().decode(this.config))
        }
}

class ConfigureResponse(id: Long) : MethodResponse("configure", id)

class PingResponse(id: Long) : MethodResponse("ping", id)

class ConfigureErrorResponse(id: Long, message: String, errorCode: Int) : MethodErrorResponse("configure", id, message, errorCode)

class Tick(val time: Long) : MethodCall("tick")

class Start(val numberOfTicks: Long, val tickResolution: Long) : MethodCall("start")

class Stop : MethodCall("stop")

class Done(val host: String, val port: Int) : MethodCall("done")

class Shutdown : MethodCall("shutdown")

class Ping : MethodCall("ping")
