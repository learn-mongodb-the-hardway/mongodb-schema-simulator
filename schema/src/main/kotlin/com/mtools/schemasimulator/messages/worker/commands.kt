package com.mtools.schemasimulator.messages.worker

import com.mtools.schemasimulator.messages.MethodCall
import com.mtools.schemasimulator.messages.MethodResponse

data class Register(val host: String, val port: Int) : MethodCall("register")

class StopResponse(id: Long) : MethodResponse("stop", id)
