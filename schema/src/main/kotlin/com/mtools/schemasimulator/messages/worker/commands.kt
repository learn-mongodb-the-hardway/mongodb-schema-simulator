package com.mtools.schemasimulator.messages.worker

import com.mtools.schemasimulator.messages.MethodCall
import com.mtools.schemasimulator.messages.MethodResponse

data class Register(val host: String, val port: Int) : MethodCall("register")

data class MetricsResult(val ticks: List<TickResult>)

data class TickResult(val tick: Long)

class StopResponse(id: Long, val metrics: MetricsResult) : MethodResponse("stop", id)
