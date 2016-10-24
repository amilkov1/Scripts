package com.weather.scripts

import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets

import com.google.common.io.CharStreams
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.{CookieStore, HttpClient}
import org.apache.http.cookie.Cookie
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.{BasicCookieStore, HttpClientBuilder}
import org.apache.http.impl.cookie.BasicClientCookie
import org.apache.http.message.BasicHeader
import org.apache.http.{Header, HttpEntity, HttpHost, HttpResponse}


/** Utilities for interacting with Apache's HttpClient and friends. */
object HttpClientUtil {

  def httpHost(name: String, port: Int): HttpHost = new HttpHost(name, port)

  def httpClientBuilder(): HttpClientBuilder = HttpClientBuilder.create()

  def uriBuilder(scheme: String, host: String, port: Int, path: String): URIBuilder =
    new URIBuilder().setScheme(scheme).setHost(host).setPort(port).setPath(path)

  def createCookieStore(): CookieStore = new BasicCookieStore

  def createCookie(domain: String, name: String, value: String): Cookie = {
    val c = new BasicClientCookie(name, value)
    c.setDomain(domain)
    c
  }

  def httpHeader(name: String, value: String): Header = new BasicHeader(name, value)

  def httpGet(client: HttpClient, uri: URI): HttpResponse = {
    client.execute(new HttpGet(uri))
  }

  def httpGet(client: HttpClient, headers: Array[Header], host: HttpHost, uri: String): HttpResponse = {
    val g = new HttpGet(uri)
    g.setHeaders(headers)
    client.execute(host, g)
  }


  def httpPost(client: HttpClient, headers: Array[Header], host: HttpHost, uri: String, content: String): HttpResponse = {
    val p = new HttpPost(uri)
    p.setHeaders(headers)
    p.setEntity(httpEntity(content))
    client.execute(host, p)
  }

  def httpPut(client: HttpClient, headers: Array[Header], host: HttpHost, uri: String, content: String): HttpResponse = {
    val p = new HttpPut(uri)
    p.setHeaders(headers)
    p.setEntity(httpEntity(content))
    client.execute(host, p)
  }

  def httpDelete(client: HttpClient, headers: Array[Header], host: HttpHost, uri: String): HttpResponse = {
    val d = new HttpDelete(uri)
    d.setHeaders(headers)
    client.execute(host, d)
  }

  def httpEntity(s: String): HttpEntity = new ByteArrayEntity(s.getBytes(StandardCharsets.UTF_8))

  def httpContentString(r: HttpResponse): String = {
    CharStreams.toString(new InputStreamReader(r.getEntity.getContent, StandardCharsets.UTF_8))
  }

  def httpStatusCode(r: HttpResponse): Int = {
    r.getStatusLine.getStatusCode
  }
}
