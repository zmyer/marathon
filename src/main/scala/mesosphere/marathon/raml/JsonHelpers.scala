package mesosphere.marathon
package raml

import play.api.libs.json.{ JsObject, Json }

trait JsonHelpers {
  def JsonObject(fields: JsField*): JsObject = Json.obj(
    fields.flatMap {
      case JsField(name, value) =>
        value.map { v =>
          name -> Json.toJsFieldJsValueWrapper(value)
        }
    }: _*
  )
  def JsonObject(fields: Seq[JsField]): JsObject = Json.obj(
    fields.flatMap {
      case JsField(name, value) =>
        value.map { v =>
          name -> Json.toJsFieldJsValueWrapper(value)
        }
    }: _*
  )
}
