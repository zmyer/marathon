package mesosphere.marathon
package storage.migration.legacy

import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state._
import mesosphere.marathon.storage.LegacyInMemConfig
import mesosphere.marathon.storage.repository.legacy.{ AppEntityRepository, GroupEntityRepository, PodEntityRepository }
import mesosphere.marathon.storage.repository.legacy.store.MarathonStore

import scala.concurrent.{ ExecutionContext, Future }

class MigrationTo1_3Test extends AkkaUnitTest {

  class Fixture {
    implicit lazy val metrics = new Metrics(new MetricRegistry)
    val maxVersions = 25
    lazy val config = LegacyInMemConfig(maxVersions)
    lazy val store = config.store

    lazy val appStore = new MarathonStore[AppDefinition](store, metrics, () => AppDefinition(id = PathId("/test")), prefix = "app:")
    lazy val appRepo = new AppEntityRepository(appStore, maxVersions = maxVersions)(ExecutionContext.global, metrics)

    lazy val podStore = new MarathonStore[PodDefinition](store, metrics, () => PodDefinition(), prefix = "pod:")
    lazy val podRepo = new PodEntityRepository(podStore, maxVersions = maxVersions)(ExecutionContext.global, metrics)

    lazy val groupStore = new MarathonStore[Group](store, metrics, () => Group.empty, prefix = "group:")
    lazy val groupRepo = new GroupEntityRepository(groupStore, maxVersions = maxVersions, appRepo, podRepo)

    lazy val migration = new MigrationTo1_3(Some(config))
  }

  val emptyGroup = Group.empty

  def constraint(field: String, operator: Option[Protos.Constraint.Operator], value: Option[String] = None) = {
    val builder = Protos.Constraint.newBuilder().setField(field)
    operator.foreach(builder.setOperator)
    value.foreach(builder.setValue)
    builder.build()
  }

  def dummyApps(): Seq[AppDefinition] = {
    import Protos.Constraint.Operator._
    val version = VersionInfo.OnlyVersion(Timestamp(1L))
    Seq(
      AppDefinition(
        id = PathId("/foo"),
        cmd = Some("foo"),
        versionInfo = version,
        constraints = Set(
          constraint("a", Some(LIKE), Some("*")) // invalid regex
        )
      ),
      AppDefinition(
        id = PathId("/bar"),
        cmd = Some("bar"),
        versionInfo = version,
        constraints = Set(
          constraint("a", Some(UNLIKE), Some("*()")) // invalid regex, we cannot fix it
        )
      ),
      AppDefinition(
        id = PathId("/qax"),
        cmd = Some("qax"),
        versionInfo = version,
        constraints = Set(
          constraint("a", Some(UNIQUE), None)
        )
      ),
      AppDefinition(
        id = PathId("/wsx"),
        cmd = Some("wsx"),
        versionInfo = version,
        constraints = Set(
          constraint("a", Some(GROUP_BY), Some("1"))
        )
      )
    )
  }

  "migration to storage format 1.3 with" when {
    "an empty migration" should {
      val f = new Fixture
      f.migration.migrate().futureValue

      "do nothing" in {
        val group = f.groupRepo.root().futureValue
        group.groups should be('empty)
        group.apps should be('empty)
        group.dependencies should be('empty)
        f.appRepo.ids().runWith(Sink.seq).futureValue should be('empty)
      }
    }

    "apps constraints" should {
      import Protos.Constraint.Operator._
      val f = new Fixture
      val apps = dummyApps()
      val root = emptyGroup.copy(apps = apps.map(app => app.id -> app).toMap)
      f.groupRepo.storeRoot(root, apps, Nil, Nil, Nil).futureValue
      Future.sequence(apps.map(f.appRepo.store)).futureValue
      f.migration.migrate().futureValue

      "be transformed, if they're invalid and we can do the transformation" in {
        val foo = f.appRepo.get(PathId("/foo")).futureValue
        foo.nonEmpty should be(true)
        foo.foreach { app =>
          app.constraints.nonEmpty should be(true)
          app.constraints should be(Set(constraint("a", Some(LIKE), Some(".*"))))
        }
      }

      "be dropped, if they're invalid and we can't do the transformation" in {
        val bar = f.appRepo.get(PathId("/bar")).futureValue
        bar.nonEmpty should be(true)
        bar.foreach { app =>
          app.constraints.nonEmpty should be(false)
        }
      }

      "be unchanged if they're valid" in {
        val qax = f.appRepo.get(PathId("/qax")).futureValue
        qax.nonEmpty should be(true)
        qax.foreach { app =>
          app.constraints should be(Set(constraint("a", Some(UNIQUE), None)))
        }
        val wsx = f.appRepo.get(PathId("/wsx")).futureValue
        wsx.nonEmpty should be(true)
        wsx.foreach { app =>
          app.constraints should be(Set(constraint("a", Some(GROUP_BY), Some("1"))))
        }
      }
    }
  }
}
