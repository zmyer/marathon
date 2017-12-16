package mesosphere.marathon
package plugin

/**
  * A Marathon RunSpec Definition
  */
trait RunSpec {

  /**
    * The uniqie id of this run specification
    */
  val id: PathId

  /**
    * The Mesos resource roles that are accepted
    */
  val acceptedResourceRoles: Set[String]

  /**
    * All defined secret definitions
    */
  val secrets: Map[String, Secret]

  /**
    * The networks that this run specification will join.
    */
  val networks: Seq[NetworkSpec]
}

/**
  * An application is a run spec that launches a single task.
  */
trait ApplicationSpec extends RunSpec {

  /**
    * The user to execute the app task
    */
  val user: Option[String]

  /**
    * The environment of this app.
    */
  val env: Map[String, EnvVarValue]

  /**
    * The labels in that app.
    */
  val labels: Map[String, String]

  /**
    * Container volumes
    */
  val volumes: Seq[AppVolumeSpec]
}

/**
  * A Marathon Container definition
  */
trait ContainerSpec {

  /**
    * The name of the container spec.
    */
  val name: String

  /**
    * The user to execute the container task
    */
  val user: Option[String]

  /**
    * The environment of this container.
    */
  val env: Map[String, EnvVarValue]

  /**
    * The labels in that container
    */
  val labels: Map[String, String]

  /**
    * Pod volume mounts.
    */
  val volumeMounts: Seq[VolumeMountSpec]
}

/**
  * A network definition.
  */
trait NetworkSpec {

  /**
    * Optional labels for a given network, may be empty.
    */
  val labels: Map[String, String]
}

/**
  * A pod is a run spec that launches a task group.
  */
trait PodSpec extends RunSpec {

  /**
    * All containers of this run specification
    */
  val containers: Seq[ContainerSpec]

  /**
    * The environment shared for all containers inside this pod.
    */
  val env: Map[String, EnvVarValue]

  /**
    * The labels in that pod.
    */
  val labels: Map[String, String]

  /**
    * Pod volumes
    */
  val podVolumes: Seq[PodVolumeSpec]
}

/**
  * Application volume definition
  */
trait AppVolumeSpec {

  val containerPath: String
}

/**
  * Application volume with a secret
  */
trait AppSecretVolumeSpec extends AppVolumeSpec {

  val secret: String
}

/**
  * Pod volume definition
  */
trait PodVolumeSpec {

  val name: String
}

/**
  * Pod volume witch a secret
  */
trait PodSecretVolumeSpec extends PodVolumeSpec {

  val secret: String
}

/**
  * Pod volume mount
  */
trait VolumeMountSpec {

  val name: String

  val mountPath: String

  val readOnly: Option[Boolean]
}
