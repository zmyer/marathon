package mesosphere.marathon.api.v2

import com.wix.accord._

import play.api.libs.json._

object Validation {

  implicit def optional[T](implicit validator: Validator[T]): Validator[Option[T]] = {
    new Validator[Option[T]] {
      override def apply(option: Option[T]): Result = option.map(validator).getOrElse(Success)
    }
  }

  implicit def every[T](implicit validator: Validator[T]): Validator[Iterable[T]] = {
    new Validator[Iterable[T]] {
      override def apply(seq: Iterable[T]): Result = {

        val violations = seq.map(item => (item, validator(item))).zipWithIndex.collect {
          case ((item, f: Failure), pos: Int) => GroupViolation(item, "not valid", Some(s"[$pos]"), f.violations)
        }

        if(violations.isEmpty) Success
        else Failure(Set(GroupViolation(seq, "seq contains elements, which are not valid", None, violations.toSet)))
      }
    }
  }

  implicit lazy val failureWrites: Writes[Failure] = Writes { f =>
      Json.obj(
        "violations" -> JsArray(violationsToJsvalue(f.violations))
      )
  }

  implicit lazy val ruleViolationWrites: Writes[RuleViolation] = Writes { v =>
      Json.obj(
        "value" -> v.value.toString,
        "constraint" -> v.constraint,
        "description" -> v.description
      )
  }

  implicit lazy val groupViolationWrites: Writes[GroupViolation] = Writes { v =>
    Json.obj(
      "value" -> v.value.toString,
      "constraint" -> v.constraint,
      "description" -> v.description,
      "children" -> JsArray(violationsToJsvalue(v.children))
    )
  }

  private def violationsToJsvalue(violations: Set[Violation]): Seq[JsValue] = {
    // TODO AW: get rid of toSeq
    violations.toSeq.map(_ match {
      case r: RuleViolation => Json.toJson(r)
      case g: GroupViolation => Json.toJson(g)
    })
  }
}
