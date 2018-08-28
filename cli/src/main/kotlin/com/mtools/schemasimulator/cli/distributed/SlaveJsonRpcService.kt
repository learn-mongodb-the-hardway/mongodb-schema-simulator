package com.mtools.schemasimulator.cli.distributed

import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcMethod
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcParam
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcService

@JsonRpcService
class SlaveJsonRpcService {
    @JsonRpcMethod
    fun registerSlave(
        @JsonRpcParam("host") host: String,
        @JsonRpcParam("port") port: Integer) {

    }
}
