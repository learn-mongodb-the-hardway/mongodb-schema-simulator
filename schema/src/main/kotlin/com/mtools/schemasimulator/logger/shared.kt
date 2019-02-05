package com.mtools.schemasimulator.logger

import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import java.net.URI

fun postMessage(uri: URI, path: String, body: String): CloseableHttpResponse {
    // Send the register message
    val httpclient = HttpClients.createDefault()
    val uri = URIBuilder()
        .setScheme("http")
        .setHost(uri.host)
        .setPort(uri.port)
        .setPath(path)
        .build()

//    RemoteWorker.logger.info { "post to $uri"}
    // Post
    val post = HttpPost(uri)
    post.entity = StringEntity(body)
    // Execute request
    return httpclient.execute(post)
}

fun getMessage(uri: URI, path: String, params: Map<String, String> = mapOf()): CloseableHttpResponse {
    // Send the register message
    val httpclient = HttpClients.createDefault()
    var builder = URIBuilder()
        .setScheme("http")
        .setHost(uri.host)
        .setPort(uri.port)
        .setPath(path)

    params.forEach { t, u ->
        builder = builder.setParameter(t, u)
    }

    val uri = builder.build()
//    RemoteWorker.logger.info { "get to $uri"}
    // Execute request
    return httpclient.execute(HttpGet(uri))
}
