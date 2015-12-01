package mesosphere.marathon.api.v2

import java.net.{ HttpURLConnection, URL, URLConnection }
import javax.validation.ConstraintViolation

import mesosphere.marathon.api.v2.json.{ V2AppDefinition, V2AppUpdate, V2Group, V2GroupUpdate }
import mesosphere.marathon.state._

import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

// Validation through wix-accord library: https://github.com/wix/accord
import com.wix.accord._
import dsl._

/**
  * Specific validation helper for specific model classes.
  */
object ModelValidation {

  //scalastyle:off null
  implicit val groupValidator = validator[Group] { group =>

  }

  implicit val groupUpdateValidator = validator[V2GroupUpdate] { groupUpdate =>
    hasOnlyOneDefinedOption(groupUpdate)
  }

  // TODO: what is the exact purpose of that one?
  private[this] def hasOnlyOneDefinedOption[A <: Product: ClassTag, B](product: A): Result = {
    val definedOptionsCount = product.productIterator.count {
      case Some(_) => true
      case _       => false
    }
    if (definedOptionsCount == 1) com.wix.accord.Success
    else failureWithRuleViolation(product, s"A conjunction of properties found", None)
  }

  def checkGroup(
    group: Group,
    path: String,
    parent: PathId,
    maxApps: Option[Int]): Result = {

    val v2Group = V2Group(group)

    val maxAppViolation = maxApps map { num =>
      failureWithRuleViolation(group,
        s"""This Marathon instance may only handle up to $num Apps!
            |(Override with command line option --max_apps)""".stripMargin,
        None)
    } getOrElse com.wix.accord.Success

    maxAppViolation.and(checkGroup(v2Group, path, parent))
  }

  def checkGroup(
    group: V2Group,
    path: String = "",
    parent: PathId = PathId.empty): Result = {
    val base = group.id.canonicalPath(parent)

    idErrors(group.id.canonicalPath(parent), group.id, LogViolation(group, "id")) and
      checkPath(parent, group.id, LogViolation(group, path + "id")) and
      checkApps(group.apps, path + "apps", base) and
      checkGroups(group.groups, path + "groups", base) and
      noAppsAndGroupsWithSameName(group, path + "apps", group.apps, group.groups) and
      noCyclicDependencies(group, path + "dependencies")
  }

  def checkGroupUpdate(
    group: V2GroupUpdate,
    needsId: Boolean,
    path: String = "",
    parent: PathId = PathId.empty): Result = {
    if (group == null) {
      failureWithRuleViolation(group, "Given group is empty!", None)
    }
    else if (group.version.isDefined || group.scaleBy.isDefined) {
      validate(group)
    }
    else {
      val base = group.id.map(_.canonicalPath(parent)).getOrElse(parent)

      group.id.map(checkPath(parent, _, LogViolation(group, path + "id"))).getOrElse(com.wix.accord.Success) and
        group.id.map(idErrors(group.groupId.canonicalPath(parent), _, LogViolation(group, "id"))).getOrElse(com.wix.accord.Success) and
        group.apps.map(checkApps(_, path + "apps", base)).getOrElse(com.wix.accord.Success) and
        group.groups.map(checkGroupUpdates(_, path + "groups", base)).getOrElse(com.wix.accord.Success)
    }
  }

  def noAppsAndGroupsWithSameName[T](
    t: T,
    path: String,
    apps: Set[V2AppDefinition],
    groups: Set[V2Group])(implicit ct: ClassTag[T]): Result = {
    val groupIds = groups.map(_.id)
    val clashingIds = apps.map(_.id).intersect(groupIds)

    if (clashingIds.isEmpty) com.wix.accord.Success
    else failureWithRuleViolation(apps, s"Groups and Applications may not have the same identifier: ${clashingIds.mkString(", ")}", Some(path))
  }

  def noCyclicDependencies(
    group: V2Group,
    path: String): Result = {
    if (group.toGroup().hasNonCyclicDependencies) com.wix.accord.Success
    else failureWithRuleViolation(group, "Dependency graph has cyclic dependencies", Some(path))
  }

  def checkGroupUpdates(
    groups: Iterable[V2GroupUpdate],
    path: String = "res",
    parent: PathId = PathId.empty): Result = {
    val results = for ((group, pos) <- groups.zipWithIndex) yield {
      checkGroupUpdate(group, needsId = true, s"$path[$pos].", parent)
    }
    results.tail.foldLeft(results.head)((res, t) => res and t)
  }

  def checkGroups(
    groups: Iterable[V2Group],
    path: String = "res",
    parent: PathId = PathId.empty): Result = {
    if(groups.isEmpty) com.wix.accord.Success
    else {
      val results = for ((group, pos) <- groups.zipWithIndex) yield {
        checkGroup(group, s"$path[$pos].", parent)
      }
      results.tail.foldLeft(results.head)((res, t) => res and t)
    }
  }

  def checkUpdates(
    apps: Iterable[V2AppUpdate],
    path: String = "res"): Result = {
    val results = for ((app, pos) <- apps.zipWithIndex) yield {
      checkUpdate(app, s"$path[$pos].")
    }
    results.tail.foldLeft(results.head)((res, t) => res and t)
  }

  def checkPath[T](
    parent: PathId,
    child: PathId,
    log: LogViolation[T]): Result = {
    val isParent = child.canonicalPath(parent).parent == parent
    if (parent != PathId.empty && !isParent)
      failureWithRuleViolation(log.obj, s"identifier $child is not child of $parent. Hint: use relative paths.", Some(log.prop))
    else com.wix.accord.Success
  }

  def checkApps(
    apps: Iterable[V2AppDefinition],
    path: String = "res",
    parent: PathId = PathId.empty): Result = {
    val results = for ((app, pos) <- apps.zipWithIndex) yield {
      checkAppConstraints(app, parent, s"$path[$pos].")
    }
    results.tail.foldLeft(results.head)((res, t) => res and t)
  }

  def checkUpdate(
    app: V2AppUpdate,
    path: String = "",
    needsId: Boolean = false): Result = {

    val test = app.id.map(validate(_))
    test.getOrElse(com.wix.accord.Success) and
      app.upgradeStrategy.map(upgradeStrategyErrors(_, LogViolation(app, "upgradeStrategy"))).getOrElse(com.wix.accord.Success) and
      app.dependencies.map(dependencyErrors(PathId.empty, _, LogViolation(app, "dependencies"))).getOrElse(com.wix.accord.Success) and
      app.storeUrls.map(urlsCanBeResolved(_, LogViolation(app, "storeUrls"))).getOrElse(com.wix.accord.Success)
  }

  def checkAppConstraints(app: V2AppDefinition, parent: PathId,
                          path: String = ""): Result = {
    idErrors(parent, app.id, LogViolation(app, path + "id")) and
      checkPath(parent, app.id, LogViolation(app, path + "id")) and
      upgradeStrategyErrors(app.upgradeStrategy, LogViolation(app, path + "upgradeStrategy")) and
      dependencyErrors(parent, app.dependencies, LogViolation(app, path + "upgradeStrategy")) and
      urlsCanBeResolved(app.storeUrls, LogViolation(app, path + "storeUrls"))
  }

  // TODO: can be isolated in app validator due to app.storeUrls
  def urlsCanBeResolved[T](urls: Seq[String], log: LogViolation[T]): Result = {
    def urlIsValid(url: String): Boolean = Try {
      new URL(url).openConnection() match {
        case http: HttpURLConnection =>
          http.setRequestMethod("HEAD")
          http.getResponseCode == HttpURLConnection.HTTP_OK
        case other: URLConnection =>
          other.getInputStream
          true //if we come here, we could read the stream
      }
    }.getOrElse(false)

    if(urls.isEmpty) com.wix.accord.Success
    else {
      val results: List[Result] = urls.toList
        .zipWithIndex
        .collect {
          case (url, pos) if !urlIsValid(url) =>
            failureWithRuleViolation(urls, s"Can not resolve url $url", Some(s"${log.prop}[$pos]"))
          case _ => com.wix.accord.Success
        }
      results.tail.foldLeft(results.head)((res, t) => res and t)
    }
  }

  case class LogViolation[T](obj: T, prop: String)

  def failureWithRuleViolation(value: scala.Any,
                               constraint: scala.Predef.String,
                               description: scala.Option[scala.Predef.String]): com.wix.accord.Failure = {
    com.wix.accord.Failure(Set(
      com.wix.accord.RuleViolation(
        value = value,
        constraint = constraint,
        description = description
      )))
  }

  /**
    * Checks if the base path of a pathId is valid.
    * @return
    */
  def idErrorBase[T](base: PathId, pathId: PathId, log: LogViolation[T]): Result = {
    Try(pathId.canonicalPath(base)) match {
      case Success(_) => com.wix.accord.Success
      case Failure(_) => failureWithRuleViolation(log.obj, s"canonical path can not be computed for ${log.prop}", Some(log.prop.toString))
    }
  }

  /**
    *
    * @param basePathId
    * @param pathId
    * @param log object and property containing the pathId for logging purposes
    * @tparam T
    * @return
    */
  def idErrors[T](basePathId: PathId, pathId: PathId, log: LogViolation[T]): Result = {
    val validPathId = validate(pathId)
    idErrorBase(basePathId, pathId, log)
  }

  def dependencyErrors[T](
    base: PathId,
    set: Set[PathId],
    log: LogViolation[T]): Result = {
    if(set.isEmpty)
      com.wix.accord.Success
    else {
      val result = validate(set)
      val results = set.zipWithIndex.map({ case (id, pos) => idErrors(base, id, log.copy(prop = s"${log.prop}[$pos]"))})

      results.tail.foldLeft(results.head)((res, t) => res and t)
    }
  }

  // TODO: app.upgradeStrategy may be implemented in an own class validator
  def upgradeStrategyErrors[T](
    upgradeStrategy: UpgradeStrategy,
    log: LogViolation[T]): Result = {
    {
      upgradeStrategy.minimumHealthCapacity match {
        case m if m < 0 => Some("is less than 0")
        case m if m > 1 => Some("is greater than 1")
        case _          => None
      }
    }.map(
      msg => failureWithRuleViolation(log.obj, s"$msg for upgrade strategy: ${upgradeStrategy.toString}", Some(log.prop + ".minimumHealthCapacity"))
    ).orElse {
        upgradeStrategy.maximumOverCapacity match {
          case m if m < 0 => Some("is less than 0")
          case m if m > 1 => Some("is greater than 1")
          case _          => None
        }
      }.map(
        msg => failureWithRuleViolation(log.obj, s"$msg for upgrade strategy: ${upgradeStrategy.toString}", Some(log.prop + ".maximumOverCapacity"))
      ) getOrElse com.wix.accord.Success
  }

  /**
    * Returns a non-empty list of validation messages if the given app definition
    * will conflict with existing apps.
    */
  def checkAppConflicts(app: AppDefinition, root: Group): Seq[String] = {
    app.containerServicePorts.toSeq.flatMap { servicePorts =>
      checkServicePortConflicts(app.id, servicePorts, root)
    }
  }

  /**
    * Returns a non-empty list of validations messages if the given app definition has service ports
    * that will conflict with service ports in other applications.
    *
    * Does not compare the app definition's service ports with the same deployed app's service ports, as app updates
    * may simply restate the existing service ports.
    */
  private def checkServicePortConflicts(appId: PathId, requestedServicePorts: Seq[Int],
                                        root: Group): Seq[String] = {

    for {
      existingApp <- root.transitiveApps.toList
      if existingApp.id != appId // in case of an update, do not compare the app against itself
      existingServicePort <- existingApp.portMappings.toList.flatten.map(_.servicePort)
      if existingServicePort != 0 // ignore zero ports, which will be chosen at random
      if requestedServicePorts contains existingServicePort
    } yield s"Requested service port $existingServicePort conflicts with a service port in app ${existingApp.id}"
  }

  /*
 * Validation specific exceptions
 */
  abstract class ModelValidationException(msg: String) extends Exception(msg)

  class DeploymentCanceledException(msg: String) extends ModelValidationException(msg)
  class AppStartCanceledException(msg: String) extends ModelValidationException(msg)
  class AppStopCanceledException(msg: String) extends ModelValidationException(msg)
  class ResolveArtifactsCanceledException(msg: String) extends ModelValidationException(msg)
}
