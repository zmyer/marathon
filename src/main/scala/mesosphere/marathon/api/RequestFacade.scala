package mesosphere.marathon
package api

import javax.servlet.http.HttpServletRequest

import mesosphere.marathon.plugin.http.HttpRequest
import mesosphere.marathon.stream.Implicits._

class RequestFacade(request: HttpServletRequest, path: String) extends HttpRequest {
  def this(request: HttpServletRequest) = this(request, request.getRequestURI)
  // Jersey will not allow calls to the request object from another thread
  // To circumvent that, we have to copy all data during creation
  val headers: Map[String, Seq[String]] = request.getHeaderNames.seq.map(header =>
    header.toLowerCase -> request.getHeaders(header).seq)(collection.breakOut)
  val cookies = request.getCookies
  val params = request.getParameterMap
  val remoteAddr = request.getRemoteAddr
  val remotePort = request.getRemotePort
  val localAddr = request.getLocalAddr
  val localPort = request.getLocalPort
  override def header(name: String): Seq[String] = headers.getOrElse(name.toLowerCase, Seq.empty)
  override def requestPath: String = path
  override def cookie(name: String): Option[String] = cookies.find(_.getName == name).map(_.getValue)
  override def queryParam(name: String): Seq[String] = Option(params.get(name)).map(_.to[Seq]).getOrElse(Seq.empty)
  val method: String = request.getMethod
}
