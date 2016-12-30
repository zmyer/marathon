package mesosphere.marathon
package core.leadership

import akka.actor.{ ActorRef, ActorSystem, Props }
import com.typesafe.config.ConfigFactory
import mesosphere.marathon.core.base.{ BaseModule, ShutdownHooks }
import mesosphere.marathon.test.Mockito

/**
  * Provides an implementation of the [[LeadershipModule]] which assumes that we are always the leader.
  *
  * This simplifies tests.
  */
object AlwaysElectedLeadershipModule extends Mockito {
  /**
    * Create a leadership module. The caller must ensure that shutdownHooks.shutdown is called so
    * that the underlying actor system is freed.
    */
  def apply(shutdownHooks: ShutdownHooks): LeadershipModule = {
    forActorsModule(BaseModule(ConfigFactory.load(), shutdownHooks = shutdownHooks))
  }

  /**
    * Create a leadership module using the given actorSystem. The caller must shutdown the given actor system
    * itself after usage.
    */
  def forActorSystem(overrideActorSystem: ActorSystem): LeadershipModule = {
    forActorsModule(new BaseModule(ConfigFactory.load()) {
      override implicit lazy val actorSystem: ActorSystem = overrideActorSystem
    })
  }

  private[this] def forActorsModule(baseModule: BaseModule = new BaseModule(
    ConfigFactory.load(),
    shutdownHooks = ShutdownHooks())): LeadershipModule =
    {
      new AlwaysElectedLeadershipModule(baseModule)
    }
}

private class AlwaysElectedLeadershipModule(actorsModule: BaseModule) extends LeadershipModule {
  override def startWhenLeader(props: Props, name: String): ActorRef =
    actorsModule.actorRefFactory.actorOf(props, name)
  override def coordinator(): LeadershipCoordinator = ???
}
