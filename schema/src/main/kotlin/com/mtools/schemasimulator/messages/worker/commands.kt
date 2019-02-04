package com.mtools.schemasimulator.messages.worker

import com.mtools.schemasimulator.logger.LogEntry
import com.mtools.schemasimulator.messages.MethodCall
import com.mtools.schemasimulator.messages.MethodResponse

data class Register(val host: String, val port: Int) : MethodCall("register")

data class MetricsResult(val ticks: List<LogEntry>) : MethodCall("metrics")

class StopResponse(id: Long) : MethodResponse("stop", id)
