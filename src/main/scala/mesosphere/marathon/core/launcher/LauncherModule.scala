package mesosphere.marathon
package core.launcher

import java.time.Clock

import akka.stream.scaladsl.SourceQueue
import mesosphere.marathon.core.launcher.impl.{ InstanceOpFactoryImpl, OfferProcessorImpl, TaskLauncherImpl }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.task.tracker.InstanceStateOpProcessor
import org.apache.mesos.Protos.Offer

/**
  * This module contains the glue code between matching tasks to resource offers
  * and actually launching the matched tasks.
  */
class LauncherModule(
    conf: MarathonConf,
    stateOpProcessor: InstanceStateOpProcessor,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher,
    pluginManager: PluginManager,
    offerStreamInput: SourceQueue[Offer]
)(implicit clock: Clock) {

  lazy val offerProcessor: OfferProcessor =
    new OfferProcessorImpl(
      conf,
      offerMatcher, taskLauncher, stateOpProcessor,
      offerStreamInput
    )

  lazy val taskLauncher: TaskLauncher = new TaskLauncherImpl(
    marathonSchedulerDriverHolder)

  lazy val taskOpFactory: InstanceOpFactory = new InstanceOpFactoryImpl(conf, pluginManager)
}
