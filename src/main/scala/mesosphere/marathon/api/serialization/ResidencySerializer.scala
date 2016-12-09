package mesosphere.marathon.api.serialization

import mesosphere.marathon.Protos.ResidencyDefinition
import mesosphere.marathon.state.Residency

object ResidencySerializer {
  def toProto(): ResidencyDefinition = ResidencyDefinition.newBuilder()
    .build()

  def fromProto(proto: ResidencyDefinition): Residency = {
    val relaunchEscalationTimeoutSeconds = if (proto.hasRelaunchEscalationTimeoutSeconds)
      proto.getRelaunchEscalationTimeoutSeconds else Residency.defaultRelaunchEscalationTimeoutSeconds

    val taskLostBehavior = if (proto.hasTaskLostBehavior)
      proto.getTaskLostBehavior else Residency.defaultTaskLostBehaviour

    Residency(relaunchEscalationTimeoutSeconds, taskLostBehavior)
  }

}
