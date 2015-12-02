package mesosphere.marathon.api.v2

import java.net.{URLConnection, HttpURLConnection, URL}

import com.wix.accord._

import scala.util.Try

object GenericValidation {
  implicit def noneOrValid[T](implicit validator: Validator[T]): Validator[Option[T]] = {
    new Validator[Option[T]] {
      override def apply(option: Option[T]): Result = option.map(validator).getOrElse(Success)
    }
  }

  implicit def seqValidator[T](implicit validator: Validator[T]): Validator[Seq[T]] = {
    new Validator[Seq[T]] {
      override def apply(seq: Seq[T]): Result = {

        val violations = seq.map(item => (item, validator(item))).zipWithIndex.collect {
          case ((item, f: Failure), pos: Int) => GroupViolation(item, "not valid", Some(s"[$pos]"), f.violations)
        }

        if(violations.isEmpty) Success
        else Failure(Set(GroupViolation(seq, "seq contains elements, which are not valid", None, violations.toSet)))
      }
    }
  }

  implicit def setValidator[T](implicit validator: Validator[T]): Validator[Set[T]] = {
    new Validator[Set[T]] {
      override def apply(seq: Set[T]): Result = {

        val violations = seq.map(item => (item, validator(item))).zipWithIndex.collect {
          case ((item, f: Failure), pos: Int) => GroupViolation(item, "not valid", Some(s"[$pos]"), f.violations)
        }

        if(violations.isEmpty) Success
        else Failure(Set(GroupViolation(seq, "seq contains elements, which are not valid", None, violations.toSet)))
      }
    }
  }

  def urlsCanBeResolvedValidator: Validator[String] = {
    new Validator[String] {
      def apply(url: String) = {
        Try {
          new URL(url).openConnection() match {
            case http: HttpURLConnection =>
              http.setRequestMethod("HEAD")
              if(http.getResponseCode == HttpURLConnection.HTTP_OK) Success
              else Failure(Set(RuleViolation(url, "url could not be resolved", None)))
            case other: URLConnection =>
              other.getInputStream
              Success //if we come here, we could read the stream
          }
        }.getOrElse(
          Failure(Set(RuleViolation(url, "url could not be resolved", None)))
        )
      }
    }
  }
}
