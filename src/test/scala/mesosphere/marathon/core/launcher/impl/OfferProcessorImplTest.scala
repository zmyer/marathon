package mesosphere.marathon
package core.launcher.impl

import mesosphere.UnitTest
import mesosphere.marathon.test.SettableClock
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.TestInstanceBuilder._
import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }
import mesosphere.marathon.core.instance.{ Instance, TestInstanceBuilder }
import mesosphere.marathon.core.launcher.{ InstanceOp, OfferProcessorConfig, TaskLauncher }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpSource, InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.state.{ AgentInfoPlaceholder, NetworkInfoPlaceholder }
import mesosphere.marathon.core.task.tracker.InstanceStateOpProcessor
import mesosphere.marathon.state.PathId
import mesosphere.marathon.test.MarathonTestHelper
import mesosphere.marathon.util.NoopSourceQueue
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class OfferProcessorImplTest extends UnitTest {
  private[this] val offer = MarathonTestHelper.makeBasicOffer().build()
  private[this] val offerId = offer.getId
  private val appId: PathId = PathId("/testapp")
  private[this] val instanceId1 = Instance.Id.forRunSpec(appId)
  private[this] val instanceId2 = Instance.Id.forRunSpec(appId)
  private[this] val taskInfo1 = MarathonTestHelper.makeOneCPUTask(Task.Id.forInstanceId(instanceId1, None)).build()
  private[this] val taskInfo2 = MarathonTestHelper.makeOneCPUTask(Task.Id.forInstanceId(instanceId2, None)).build()
  private[this] val instance1 = TestInstanceBuilder.newBuilderWithInstanceId(instanceId1).addTaskWithBuilder().taskFromTaskInfo(taskInfo1).build().getInstance()
  private[this] val instance2 = TestInstanceBuilder.newBuilderWithInstanceId(instanceId2).addTaskWithBuilder().taskFromTaskInfo(taskInfo2).build().getInstance()
  private[this] val task1: Task.LaunchedEphemeral = instance1.appTask
  private[this] val task2: Task.LaunchedEphemeral = instance2.appTask

  private[this] val tasks = Seq((taskInfo1, task1, instance1), (taskInfo2, task2, instance2))
  private[this] val arbitraryInstanceUpdateEffect = InstanceUpdateEffect.Noop(instanceId1)

  case class Fixture(
      conf: OfferProcessorConfig = new OfferProcessorConfig { verify() },
      clock: SettableClock = new SettableClock(),
      offerMatcher: OfferMatcher = mock[OfferMatcher],
      taskLauncher: TaskLauncher = mock[TaskLauncher],
      stateOpProcessor: InstanceStateOpProcessor = mock[InstanceStateOpProcessor]) {
    val offerProcessor = new OfferProcessorImpl(
      conf, offerMatcher, taskLauncher, stateOpProcessor,
      NoopSourceQueue()
    )
  }

  object f {
    import org.apache.mesos.{ Protos => Mesos }
    val launch = new InstanceOpFactoryHelper(Some("principal"), Some("role")).launchEphemeral(_: Mesos.TaskInfo, _: Task.LaunchedEphemeral, _: Instance)
    val launchWithNewTask = new InstanceOpFactoryHelper(Some("principal"), Some("role")).launchOnReservation _
  }

  class DummySource extends InstanceOpSource {
    var rejected = Vector.empty[(InstanceOp, String)]
    var accepted = Vector.empty[InstanceOp]

    override def instanceOpRejected(op: InstanceOp, reason: String): Unit = rejected :+= op -> reason
    override def instanceOpAccepted(op: InstanceOp): Unit = accepted :+= op
  }

  "OfferProcessorImpl" should {
    "match successful, launch tasks successful" in new Fixture {
      Given("an offer")
      val dummySource = new DummySource
      val tasksWithSource = tasks.map(task => InstanceOpWithSource(dummySource, f.launch(task._1, task._2, task._3)))

      And("a cooperative offerMatcher and taskTracker")
      offerMatcher.matchOffer(offer) returns Future.successful(MatchedInstanceOps(offerId, tasksWithSource))
      for (task <- tasks) {
        val stateOp = InstanceUpdateOperation.LaunchEphemeral(task._3)
        stateOpProcessor.process(stateOp) returns Future.successful(arbitraryInstanceUpdateEffect)
      }

      And("a working taskLauncher")
      val ops: Seq[InstanceOp] = tasksWithSource.map(_.op)
      taskLauncher.acceptOffer(offerId, ops) returns true

      When("processing the offer")
      offerProcessor.processOffer(offer).futureValue(Timeout(1.second))

      Then("we saw the offerMatch request and the task launches")
      verify(offerMatcher).matchOffer(offer)
      verify(taskLauncher).acceptOffer(offerId, ops)

      And("all task launches have been accepted")
      assert(dummySource.rejected.isEmpty)
      assert(dummySource.accepted == tasksWithSource.map(_.op))

      And("the tasks have been stored")
      for (task <- tasksWithSource) {
        val ordered = inOrder(stateOpProcessor)
        ordered.verify(stateOpProcessor).process(task.op.stateOp)
      }
    }

    "match successful, launch tasks unsuccessful" in new Fixture {
      Given("an offer")
      val dummySource = new DummySource
      val tasksWithSource = tasks.map(task => InstanceOpWithSource(dummySource, f.launch(task._1, task._2, task._3)))

      And("a cooperative offerMatcher and taskTracker")
      offerMatcher.matchOffer(offer) returns Future.successful(MatchedInstanceOps(offerId, tasksWithSource))
      for (task <- tasksWithSource) {
        val op = task.op
        stateOpProcessor.process(op.stateOp) returns Future.successful(arbitraryInstanceUpdateEffect)
        stateOpProcessor.process(InstanceUpdateOperation.ForceExpunge(op.stateOp.instanceId)) returns Future.successful(arbitraryInstanceUpdateEffect)
      }

      And("a dysfunctional taskLauncher")
      taskLauncher.acceptOffer(offerId, tasksWithSource.map(_.op)) returns false

      When("processing the offer")
      offerProcessor.processOffer(offer).futureValue(Timeout(1.second))

      Then("we saw the matchOffer request and the task launch attempt")
      verify(offerMatcher).matchOffer(offer)
      verify(taskLauncher).acceptOffer(offerId, tasksWithSource.map(_.op))

      And("all task launches were rejected")
      assert(dummySource.accepted.isEmpty)
      assert(dummySource.rejected.map(_._1) == tasksWithSource.map(_.op))

      And("the tasks where first stored and then expunged again")
      for (task <- tasksWithSource) {
        val ordered = inOrder(stateOpProcessor)
        val op = task.op
        ordered.verify(stateOpProcessor).process(op.stateOp)
        ordered.verify(stateOpProcessor).process(InstanceUpdateOperation.ForceExpunge(op.stateOp.instanceId))
      }
    }

    "match successful, launch tasks unsuccessful, revert to prior task state" in new Fixture {
      Given("an offer")
      val dummySource = new DummySource
      val tasksWithSource = tasks.map {
        case (taskInfo, _, _) =>
          val dummyInstance = TestInstanceBuilder.newBuilder(appId).addTaskResidentReserved().getInstance()
          val updateOperation = InstanceUpdateOperation.LaunchOnReservation(
            instanceId = dummyInstance.instanceId,
            newTaskId = Task.Id.forResidentTask(Task.Id(taskInfo.getTaskId)),
            runSpecVersion = clock.now(),
            timestamp = clock.now(),
            status = Task.Status(clock.now(), condition = Condition.Running, networkInfo = NetworkInfoPlaceholder()),
            hostPorts = Seq.empty,
            agentInfo = AgentInfoPlaceholder())
          val launch = f.launchWithNewTask(
            taskInfo,
            updateOperation,
            dummyInstance
          )
          InstanceOpWithSource(dummySource, launch)
      }

      And("a cooperative offerMatcher and taskTracker")
      offerMatcher.matchOffer(offer) returns Future.successful(MatchedInstanceOps(offerId, tasksWithSource))
      for (task <- tasksWithSource) {
        val op = task.op
        stateOpProcessor.process(op.stateOp) returns Future.successful(arbitraryInstanceUpdateEffect)
        stateOpProcessor.process(InstanceUpdateOperation.Revert(op.oldInstance.get)) returns Future.successful(arbitraryInstanceUpdateEffect)
      }

      And("a dysfunctional taskLauncher")
      taskLauncher.acceptOffer(offerId, tasksWithSource.map(_.op)) returns false

      When("processing the offer")
      offerProcessor.processOffer(offer).futureValue(Timeout(1.second))

      Then("we saw the matchOffer request and the task launch attempt")
      verify(offerMatcher).matchOffer(offer)
      verify(taskLauncher).acceptOffer(offerId, tasksWithSource.map(_.op))

      And("all task launches were rejected")
      assert(dummySource.accepted.isEmpty)
      assert(dummySource.rejected.map(_._1) == tasksWithSource.map(_.op))

      And("the tasks where first stored and then expunged again")
      for (task <- tasksWithSource) {
        val op = task.op
        val ordered = inOrder(stateOpProcessor)
        ordered.verify(stateOpProcessor).process(op.stateOp)
        ordered.verify(stateOpProcessor).process(InstanceUpdateOperation.Revert(op.oldInstance.get))
      }
    }

    "match empty => decline" in new Fixture {
      offerMatcher.matchOffer(offer) returns Future.successful(MatchedInstanceOps(offerId, Seq.empty))

      offerProcessor.processOffer(offer).futureValue(Timeout(1.second))

      verify(offerMatcher).matchOffer(offer)
      verify(taskLauncher).declineOffer(offerId, refuseMilliseconds = Some(conf.declineOfferDuration()))
    }

    "match crashed => decline" in new Fixture {
      offerMatcher.matchOffer(offer) returns Future.failed(new RuntimeException("failed matching"))

      offerProcessor.processOffer(offer).futureValue(Timeout(1.second))

      verify(offerMatcher).matchOffer(offer)
      verify(taskLauncher).declineOffer(offerId, refuseMilliseconds = None)
    }

  }
}
