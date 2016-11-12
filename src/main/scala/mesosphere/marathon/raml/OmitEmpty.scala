package mesosphere.marathon
package raml

import play.api.libs.json.{ JsObject, Writes => JsonWrites }

trait OmitEmpty {
  import scala.language.implicitConversions

  implicit def Seq2JsField[T](field: (String, Seq[T], Boolean))(implicit w: JsonWrites[Seq[T]]): JsField = field match {
    case (name, Nil, omitEmpty) if omitEmpty => JsField(name, None)
    case (name, t, omitEmpty) => JsField(name, Some(w.writes(t)))
  }

  implicit def Set2JsField[T](field: (String, Set[T], Boolean))(implicit w: JsonWrites[Set[T]]): JsField = field match {
    case (name, t, omitEmpty) if t.isEmpty && omitEmpty => JsField(name, None)
    case (name, t, omitEmpty) => JsField(name, Some(w.writes(t)))
  }

  implicit def Map2JsField[K, V](field: (String, Map[K, V], Boolean))(implicit w: JsonWrites[Map[K, V]]): JsField = field match {
    case (name, t, omitEmpty) if t.isEmpty && omitEmpty => JsField(name, None)
    case (name, t, omitEmpty) => JsField(name, Some(w.writes(t)))
  }

  implicit def Option2JsField[T](field: (String, Option[T], Boolean))(implicit w: JsonWrites[Option[T]]): JsField = field match {
    case (name, None, omitEmpty) if omitEmpty => JsField(name, None)
    case (name, t, omitEmpty) => JsField(name, Some(w.writes(t)))
  }

  implicit def JsObj2JsField(field: (String, JsObject, Boolean)): JsField = {
    val (name, obj, omitEmpty) = field
    if (obj.fields.isEmpty && omitEmpty) JsField(name, None)
    else JsField(name, Some(obj))
  }

  implicit def Any2JsField[T](field: (String, T, Boolean))(implicit w: JsonWrites[T]): JsField =
    // custom types that want to opt-in to OmitEmpty support should extend OmitEmptyWrites and provide an implicit
    // conversion from the custom type to JsField.
    JsField(field._1, Some(w.writes(field._2)))
}

trait OmitEmptyWrites[A] extends JsonWrites[A] with OmitEmpty
