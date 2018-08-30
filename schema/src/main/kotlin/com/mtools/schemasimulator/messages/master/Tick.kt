package com.mtools.schemasimulator.messages.master

import com.mtools.schemasimulator.messages.MethodCall

class Tick(val time: Long) : MethodCall("configure")
