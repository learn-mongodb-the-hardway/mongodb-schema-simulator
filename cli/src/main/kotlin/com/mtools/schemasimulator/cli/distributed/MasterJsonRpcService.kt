package com.mtools.schemasimulator.cli.distributed

import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcMethod
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcParam
import com.github.arteam.simplejsonrpc.core.annotation.JsonRpcService

@JsonRpcService
class MasterJsonRpcService {

    @JsonRpcMethod
    fun registerSlave(
        @JsonRpcParam("host") host: String,
        @JsonRpcParam("port") port: Integer) {

    }

    fun started(
        @JsonRpcParam("host") host: String,
        @JsonRpcParam("port") port: Integer) {

    }

    fun report(
        @JsonRpcParam("host") host: String,
        @JsonRpcParam("port") port: Integer) {

    }

    fun done(
        @JsonRpcParam("host") host: String,
        @JsonRpcParam("port") port: Integer) {

    }
}

//@JsonRpcError(code = -32032, "")
//class MasterJsonRpcServiceException(message: String, ex: Throwable?): Exception(message, ex)
