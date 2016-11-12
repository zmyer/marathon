package mesosphere.marathon
package raml

import play.api.libs.json.JsValue

case class JsField(name: String, value: Option[JsValue])
