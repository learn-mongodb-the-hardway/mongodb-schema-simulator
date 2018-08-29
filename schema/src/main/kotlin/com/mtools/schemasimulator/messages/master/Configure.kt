package com.mtools.schemasimulator.messages.master

import com.mtools.schemasimulator.messages.MethodCall

data class Configure(val name: String, val config: String) : MethodCall("configure")
