package com.mtools.schemasimulator.messages.slave

import com.mtools.schemasimulator.messages.MethodCall

data class Register(val host: String, val port: Int) : MethodCall("register")
