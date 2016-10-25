package mesosphere.marathon
package storage.migration.legacy

import java.util.regex.Pattern

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.{ AppDefinition, Group, Timestamp }
import mesosphere.marathon.storage.LegacyStorageConfig
import mesosphere.marathon.storage.repository.{ AppRepository, DeploymentRepository, GroupRepository, PodRepository }
import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.upgrade.DeploymentPlan
import org.slf4j.LoggerFactory

import scala.async.Async.{ async, await }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

/**
  * Fix apps that use invalid constraints.
  */
class MigrationTo1_3_4(legacyConfig: Option[LegacyStorageConfig])(implicit
  ctx: ExecutionContext,
    metrics: Metrics,
    mat: Materializer) {

  private[this] val log = LoggerFactory.getLogger(getClass)

  /**
    * check for invalid regular expression's used in constraint values with LIKE and UNLIKE operators.
    * when we find an invalid one, try to fix it if it's simple; otherwise drop the constraint from the app to avoid
    * dropping the whole app definition on the floor.
    */
  def fixConstraints(app: AppDefinition): AppDefinition = {
    import Constraint.Operator._
    val newConstraints: Set[Option[Constraint]] = app.constraints.map { constraint =>
      if (!(constraint.hasValue && constraint.hasOperator)) {
        Some(constraint)
      } else {
        constraint.getOperator match {
          case LIKE | UNLIKE =>
            val cValue = constraint.getValue
            Try(Pattern.compile(cValue)) match {
              case scala.util.Success(_) =>
                Some(constraint)
              case scala.util.Failure(_) if cValue == "*" =>
                // this seems to be the most common case that people hit
                log.info(s"fixing incorrect regexp constraint for app ${app.id}")
                Some(constraint.toBuilder.setValue(".*").build())
              case scala.util.Failure(_) =>
                // not sure how we're supposed to automatically fix other invalid regular expressions.
                // for now, don't remove the app; just remove the bad constraint and let the user update the app later.
                log.error(s"detected invalid constraint value for app ${app.id}: '${constraint}'; removing constraint")
                None
            }
          case _ =>
            Some(constraint)
        }
      }
    }
    app.copy(constraints = newConstraints.flatten)
  }

  @SuppressWarnings(Array("all")) // async/await
  def migrate(): Future[Unit] =
    legacyConfig.fold(Future.successful(())) { config =>
      async {
        log.info("Start 1.3 migration")

        val deploymentRepo = DeploymentRepository.legacyRepository(config.entityStore[DeploymentPlan]).store
        val appRepository = AppRepository.legacyRepository(config.entityStore[AppDefinition], config.maxVersions)
        val podRepository = PodRepository.legacyRepository(config.entityStore[PodDefinition], config.maxVersions)
        val groupRepository =
          GroupRepository.legacyRepository(config.entityStore[Group], config.maxVersions, appRepository, podRepository)
        implicit val groupOrdering = Ordering.by[Group, Timestamp](_.version)

        val deploymentNames = await(deploymentRepo.names())
        val deployments: Map[String, DeploymentPlan] = await(Future.sequence(deploymentNames.map { name =>
          deploymentRepo.fetch(name).collect { case Some(d) => name -> d }
        })).toMap

        await(Future.sequence(deployments.collect {
          case (name, deployment) =>
            // warning: non-tailrec recursion ahead..
            def fixGroup(g: Group): Group = g.copy(
              apps = g.apps.mapValues(fixConstraints),
              groupsById = g.groupsById.mapValues(fixGroup)
            )
            val updated = deployment.copy(
              original = fixGroup(deployment.original),
              target = fixGroup(deployment.target))
            deploymentRepo.store(name, updated)
        }))

        val groupVersions = await {
          groupRepository.rootVersions().mapAsync(Int.MaxValue) { version =>
            groupRepository.rootVersion(version)
          }.collect { case Some(r) => r }.runWith(Sink.seq).map(_.sorted)
        }

        val storeHistoricalApps = Future.sequence(
          groupVersions.flatMap { version =>
            version.transitiveApps.map { app =>
              appRepository.storeVersion(fixConstraints(app))
            }
          }
        )
        val storeUpdatedVersions = Future.sequence(groupVersions.map(groupRepository.storeVersion))
        await(storeHistoricalApps)
        await(storeUpdatedVersions)

        val root = await(groupRepository.root())
        await(groupRepository.storeRoot(
          root, root.transitiveApps.map(fixConstraints).toIndexedSeq, Nil, Nil, Nil))

        log.info("Finished 1.3 migration")
        ()
      }
    }
}
