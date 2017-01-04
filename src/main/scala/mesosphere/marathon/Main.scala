package mesosphere.marathon

import java.lang.Thread.UncaughtExceptionHandler

import com.google.common.util.concurrent.ServiceManager
import com.google.inject.{Guice, Module}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.chaos.http.{HttpModule, HttpService}
import mesosphere.chaos.metrics.MetricsModule
import mesosphere.marathon.api.MarathonRestModule
import mesosphere.marathon.api.v2.json.AppAndGroupFormats
import mesosphere.marathon.core.base._
import mesosphere.marathon.core.CoreGuiceModule
import mesosphere.marathon.core.base.toRichRuntime
import mesosphere.marathon.metrics.{MetricsReporterModule, MetricsReporterService}
import mesosphere.marathon.stream._
import mesosphere.mesos.LibMesos
import org.slf4j.LoggerFactory
import org.slf4j.bridge.SLF4JBridgeHandler

import scala.concurrent.ExecutionContext

class MarathonApp(args: Seq[String]) extends AutoCloseable with StrictLogging {
  private var running = false
  private val log = LoggerFactory.getLogger(getClass.getName)

  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()
  Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
    override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
      logger.error(s"Terminating ${conf.httpPort()} due to uncaught exception in thread ${thread.getName}:${thread.getId}", throwable)
      Runtime.getRuntime.asyncExit()(ExecutionContext.global)
    }
  })

  protected def modules: Seq[Module] = {
    Seq(
      new HttpModule(conf),
      new MetricsModule,
      new MetricsReporterModule(conf),
      new MarathonModule(conf, conf),
      new MarathonRestModule,
      new DebugModule(conf),
      new CoreGuiceModule
    )
  }
  private var serviceManager: Option[ServiceManager] = None

  private val EnvPrefix = "MARATHON_CMD_"
  private lazy val envArgs: Array[String] = {
    sys.env.withFilter(_._1.startsWith(EnvPrefix)).flatMap {
      case (key, value) =>
        val argKey = s"--${key.replaceFirst(EnvPrefix, "").toLowerCase.trim}"
        if (value.trim.length > 0) Seq(argKey, value) else Seq(argKey)
    }(collection.breakOut)
  }

  val conf = {
    new AllConf(args ++ envArgs)
  }

  def start(): Unit = if (!running) {
    running = true
    setConcurrentContextDefaults()

    log.info(s"Starting Marathon ${BuildInfo.version}/${BuildInfo.buildref} with ${args.mkString(" ")}")

    if (LibMesos.isCompatible) {
      log.info(s"Successfully loaded libmesos: version ${LibMesos.version}")
    } else {
      log.error(s"Failed to load libmesos: ${LibMesos.version}")
      System.exit(1)
    }

    val injector = Guice.createInjector(modules)
    val services = Seq(
      classOf[HttpService],
      classOf[MarathonSchedulerService],
      classOf[MetricsReporterService]).map(injector.getInstance(_))
    serviceManager = Some(new ServiceManager(services))

    sys.addShutdownHook(shutdownAndWait())

    serviceManager.foreach(_.startAsync())

    try {
      serviceManager.foreach(_.awaitHealthy())
    } catch {
      case e: Exception =>
        log.error(s"Failed to start all services. Services by state: ${serviceManager.map(_.servicesByState()).getOrElse("[]")}", e)
        shutdownAndWait()
        throw e
    }

    log.info("All services up and running.")
  }

  def shutdown(): Unit = if (running) {
    running = false
    log.info("Shutting down services")
    serviceManager.foreach(_.stopAsync())
  }

  def shutdownAndWait(): Unit = {
    serviceManager.foreach { serviceManager =>
      shutdown()
      log.info("Waiting for services to shut down")
      serviceManager.awaitStopped()
    }
  }

  override def close(): Unit = shutdownAndWait()

  /**
    * Make sure that we have more than one thread -- otherwise some unmarked blocking operations might cause trouble.
    *
    * See
    * [The Global Execution
    *  Context](http://docs.scala-lang.org/overviews/core/futures.html#the-global-execution-context)
    * in the scala documentation.
    *
    * Here is the relevant excerpt in case the link gets broken:
    *
    * # The Global Execution Context
    *
    * ExecutionContext.global is an ExecutionContext backed by a ForkJoinPool. It should be sufficient for most
    * situations but requires some care. A ForkJoinPool manages a limited amount of threads (the maximum amount of
    * thread being referred to as parallelism level). The number of concurrently blocking computations can exceed the
    * parallelism level only if each blocking call is wrapped inside a blocking call (more on that below). Otherwise,
    * there is a risk that the thread pool in the global execution context is starved, and no computation can proceed.
    *
    * By default the ExecutionContext.global sets the parallelism level of its underlying fork-join pool to the amount
    * of available processors (Runtime.availableProcessors). This configuration can be overriden by setting one
    * (or more) of the following VM attributes:
    *
    * scala.concurrent.context.minThreads - defaults to Runtime.availableProcessors
    * scala.concurrent.context.numThreads - can be a number or a multiplier (N) in the form ‘xN’ ;
    *                                       defaults to Runtime.availableProcessors
    * scala.concurrent.context.maxThreads - defaults to Runtime.availableProcessors
    *
    * The parallelism level will be set to numThreads as long as it remains within [minThreads; maxThreads].
    *
    * As stated above the ForkJoinPool can increase the amount of threads beyond its parallelismLevel in the
    * presence of blocking computation.
    */
  private[this] def setConcurrentContextDefaults(): Unit = {
    def setIfNotDefined(property: String, value: String): Unit = {
      if (!sys.props.contains(property)) {
        sys.props += property -> value
      }
    }

    setIfNotDefined("scala.concurrent.context.minThreads", "5")
    setIfNotDefined("scala.concurrent.context.numThreads", "x2")
    setIfNotDefined("scala.concurrent.context.maxThreads", "64")
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val app = new MarathonApp(args.toVector)
    app.start()
  }
}

object ValidateMain extends AppAndGroupFormats {
  def main(args: Array[String]): Unit = {
    // parse stdin
    val appDefinition =
    f"""{
        |   "id": "/XXXX/instance_name/backend",
        |   "apps": [
        |     {
        |       "id" : "database",
        |       "cpus" : 1.0,
        |       "mem" : 4000,
        |       "instances" : 1,
        |       "requirePorts" : true,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "volumes": [
        |            {
        |              "containerPath": "database_volume",
        |              "mode": "RW",
        |              "persistent": {
        |                "size": 75000
        |              }
        |            },
        |            {
        |              "containerPath": "/var/lib/postgresql/data",
        |              "hostPath": "database_volume",
        |              "mode": "RW"
        |            }
        |         ],
        |         "docker": {
        |           "image": "XXXXinc/XXXX-postgres:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             { "containerPort": 5432, "hostPort": 5432, "protocol": "tcp" }
        |           ]
        |         }
        |       },
        |       "env" : {
        |        "POSTGRES_PASSWORD" : "database_root_password",
        |        "XXXX_DB_USER" : "spring_datasource_username",
        |        "XXXX_DB_PASS" : "spring_datasource_password"
        |       },
        |       "healthChecks": [
        |         {
        |            "protocol": "TCP",
        |            "portIndex": 0,
        |            "gracePeriodSeconds": 600,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |         }
        |       ],
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ]
        |     },
        |     {
        |       "id": "discoveryserver",
        |       "cpus": 0.05,
        |       "mem": 2000,
        |       "instances": 1,
        |       "requirePorts": true,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "docker": {
        |           "image": "XXXXinc/XXXX-discovery-service:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             { "containerPort": 8761, "hostPort": 8761, "protocol": "tcp" }
        |           ]
        |         }
        |       },
        |       "healthChecks": [
        |         {
        |            "protocol": "HTTP",
        |            "path" : "/",
        |            "portIndex": 0,
        |            "gracePeriodSeconds": 180,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |         }
        |       ],
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ]
        |     },
        |     {
        |       "id": "configserver",
        |       "cpus": 0.01,
        |       "mem": 1000,
        |       "instances": 1,
        |       "requirePorts": false,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "docker": {
        |           "image": "XXXXinc/XXXX-config-service:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             { "containerPort": 0, "hostPort": 8888, "protocol": "tcp" }
        |           ]
        |         }
        |       },
        |       "env" : {
        |         "SPRING_PROFILES_ACTIVE" : "instance_name",
        |         "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE" : "http://discoveryserver.backend.instance_name.XXXX.marathon.mesos:8761/eureka/"
        |       },
        |       "healthChecks": [
        |         {
        |            "protocol": "HTTP",
        |            "path" : "/internal/monitor/info",
        |            "portIndex": 0,
        |            "gracePeriodSeconds": 60,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |         }
        |       ],
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ],
        |       "dependencies": [
        |         "/XXXX/instance_name/backend/discoveryserver"
        |       ]
        |     },
        |     {
        |       "id": "authserver",
        |       "cpus": 0.05,
        |       "mem": 2000,
        |       "instances": 2,
        |       "constraints": [
        |         [ "hostname", "UNIQUE" ]
        |       ],
        |       "requirePorts": false,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "docker": {
        |           "image": "XXXXinc/XXXX-backend-sso-auth:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             {
        |               "hostPort": 0,
        |               "containerPort": 8445,
        |               "servicePort" : 0
        |             }
        |           ]
        |         }
        |       },
        |       "env" : {
        |         "SPRING_PROFILES_ACTIVE" : "instance_name",
        |         "SPRING_DATASOURCE_USERNAME" : "spring_datasource_username",
        |         "SPRING_DATASOURCE_PASSWORD" : "spring_datasource_password",
        |         "SPRING_DATASOURCE_URL" : "jdbc:postgresql://database.backend.instance_name.XXXX.marathon.mesos:5432/XXXX_sso_users",
        |         "EUREKA_INSTANCE_HOSTNAME" : "authserver.backend.instance_name.XXXX.marathon.mesos",
        |         "EUREKA_INSTANCE_SECUREPORT" : "$${PORT0}",
        |         "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE" : "http://discoveryserver.backend.instance_name.XXXX.marathon.mesos:8761/eureka/"
        |       },
        |       "healthChecks": [
        |         {
        |            "protocol": "TCP",
        |            "portIndex" : 0,
        |            "gracePeriodSeconds": 120,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |         }
        |       ],
        |       "labels" : {
        |         "HAPROXY_GROUP" : "XXXX-internal-instance_name",
        |         "HAPROXY_0_FRONTEND_HEAD" : "frontend {backend}\\n  bind {bindAddr}:{servicePort} ssl crt /etc/ssl/cert.pem {bindOptions}\\n  mode {mode}\\n",
        |         "HAPROXY_0_BACKEND_SERVER_OPTIONS" : "  server {serverName} {host_ipv4}:{port}{cookieOptions} ssl verify none {healthCheckOptions}{otherOptions}\\n",
        |         "HAPROXY_0_USE_HSTS" : "true",
        |         "HAPROXY_0_REDIRECT_TO_HTTPS": "true",
        |         "HAPROXY_0_MODE" : "http"
        |       },
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ],
        |       "dependencies": [
        |         "/XXXX/instance_name/backend/database",
        |         "/XXXX/instance_name/backend/discoveryserver",
        |         "/XXXX/instance_name/backend/configserver"
        |       ],
        |       "upgradeStrategy" : {
        |          "maximumOverCapacity": 0,
        |          "minimumHealthCapacity" : 0.5
        |       }
        |     },
        |     {
        |       "id": "mainserver",
        |       "cpus": 1.0,
        |       "mem": 6000,
        |       "instances": 2,
        |       "constraints": [
        |         [ "hostname", "UNIQUE" ]
        |       ],
        |       "acceptedResourceRoles": [ "slave_public", "*" ],
        |       "requirePorts": false,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "docker": {
        |           "image": "XXXXinc/backend-mainserver:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             { "hostPort": 0, "containerPort": 8443, "servicePort": 0}
        |           ]
        |         }
        |       },
        |       "labels" : {
        |         "HAPROXY_0_VHOST": "mainserver_haproxy_vhost",
        |         "HAPROXY_GROUP" : "XXXX-external-instance_name",
        |         "HAPROXY_0_FRONTEND_HEAD" : "frontend {backend}\\n  bind {bindAddr}:{servicePort} ssl crt /etc/ssl/cert.pem {bindOptions}\\n  mode {mode}\\n",
        |         "HAPROXY_0_BACKEND_SERVER_OPTIONS" : "  server {serverName} {host_ipv4}:{port}{cookieOptions} ssl verify none {healthCheckOptions}{otherOptions}\\n",
        |         "HAPROXY_0_USE_HSTS" : "true",
        |         "HAPROXY_0_REDIRECT_TO_HTTPS": "true",
        |         "HAPROXY_0_MODE" : "http",
        |         "HAPROXY_0_STICKY" : "true"
        |       },
        |       "env" : {
        |         "SPRING_PROFILES_ACTIVE" : "instance_name",
        |         "SPRING_DATASOURCE_USERNAME" : "spring_datasource_username",
        |         "SPRING_DATASOURCE_PASSWORD" : "spring_datasource_password",
        |         "SPRING_DATASOURCE_URL" : "jdbc:postgresql://database.backend.instance_name.XXXX.marathon.mesos:5432/mainserver",
        |         "SPRING_DATASOURCE_READ-REPLICA-URL" : "jdbc:postgresql://database.backend.instance_name.XXXX.marathon.mesos:5432/mainserver",
        |         "EUREKA_INSTANCE_HOSTNAME" : "mainserver.backend.instance_name.XXXX.marathon.mesos",
        |         "EUREKA_INSTANCE_SECUREPORT" : "$${PORT0}",
        |         "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE" : "http://discoveryserver.backend.instance_name.XXXX.marathon.mesos:8761/eureka/",
        |         "SECURITY_OAUTH2_RESOURCE_USERINFOURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/user",
        |         "SECURITY_OAUTH2_RESOURCE_TOKENINFOURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/check_token",
        |         "SECURITY_OAUTH2_CLIENT_ACCESSTOKENURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/oauth/token",
        |         "SECURITY_OAUTH2_CLIENT_USERAUTHORIZATIONURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/oauth/authorize",
        |         "SECURITY_OAUTH2_CLIENT_CLIENTID" : "mainserver_oauth_clientid",
        |         "SECURITY_OAUTH2_CLIENT_CLIENTSECRET" : "mainserver_oauth_clientsecret",
        |         "XXXX_SETTINGS_SERVER" : "mainserver_api_host"
        |       },
        |       "healthChecks": [
        |         {
        |            "protocol": "TCP",
        |            "portIndex" : 0,
        |            "gracePeriodSeconds": 180,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |         }
        |       ],
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url}",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ],
        |       "dependencies": [
        |         "/XXXX/instance_name/backend/database",
        |         "/XXXX/instance_name/backend/discoveryserver",
        |         "/XXXX/instance_name/backend/configserver",
        |         "/XXXX/instance_name/backend/authserver"
        |       ],
        |       "upgradeStrategy" : {
        |          "maximumOverCapacity": 0,
        |          "minimumHealthCapacity" : 0.5
        |       }
        |     },
        |     {
        |       "id": "batch",
        |       "cpus": 1.0,
        |       "mem": 8000,
        |       "instances": 1,
        |       "acceptedResourceRoles": [ "slave_public", "*" ],
        |       "requirePorts": false,
        |       "args" : [],
        |       "container": {
        |         "type": "DOCKER",
        |         "docker": {
        |           "image": "XXXXinc/XXXX-batch:docker_tag",
        |           "network": "BRIDGE",
        |           "forcePullImage": true,
        |           "portMappings": [
        |             { "hostPort": 0, "containerPort": 8444, "protocol": "tcp" }
        |           ]
        |         }
        |       },
        |       "env" : {
        |         "SPRING_PROFILES_ACTIVE" : "instance_name",
        |         "SPRING_DATASOURCE_USERNAME" : "spring_datasource_username",
        |         "SPRING_DATASOURCE_PASSWORD" : "spring_datasource_password",
        |         "SPRING_DATASOURCE_URL" : "jdbc:postgresql://database.backend.instance_name.XXXX.marathon.mesos:5432/spring_batch",
        |         "SPRING_DATA_CASSANDRA_CONTACT-POINTS" : "node-0.XXXX-cassandra-instance_name.mesos,node-1.XXXX-cassandra-instance_name.mesos,node-2.XXXX-cassandra-instance_name.mesos",
        |         "EUREKA_INSTANCE_HOSTNAME" : "batch.backend.instance_name.XXXX.marathon.mesos",
        |         "EUREKA_INSTANCE_SECUREPORT" : "$${PORT0}",
        |         "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE" : "http://discoveryserver.backend.instance_name.XXXX.marathon.mesos:8761/eureka/",
        |         "XXXX_REST_SERVER" : "https://mainserver.backend.instance_name.XXXX.marathon.mesos:8443",
        |         "SECURITY_OAUTH2_RESOURCE_USERINFOURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/user",
        |         "SECURITY_OAUTH2_RESOURCE_TOKENINFOURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/check_token",
        |         "SECURITY_OAUTH2_CLIENT_ACCESSTOKENURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/oauth/token",
        |         "SECURITY_OAUTH2_CLIENT_USERAUTHORIZATIONURI" : "https://lb-private.instance_name.XXXX.marathon.mesos:authserver_service_port/oauth/authorize",
        |         "SECURITY_OAUTH2_CLIENT_CLIENTID" : "bhp_oauth_clientid",
        |         "SECURITY_OAUTH2_CLIENT_CLIENTSECRET" : "bhp_oauth_clientsecret"
        |       },
        |       "healthChecks": [
        |          {
        |            "protocol": "TCP",
        |            "portIndex" : 0,
        |            "gracePeriodSeconds": 60,
        |            "timeoutSeconds": 5,
        |            "intervalSeconds": 10,
        |            "maxConsecutiveFailures": 3
        |          }
        |       ],
        |       "fetch" : [
        |         {
        |            "uri" : "docker_credentials_url",
        |            "executable" : false,
        |            "extract" : true,
        |            "cache" : true
        |         }
        |       ],
        |       "dependencies": [
        |         "/XXXX-cassandra-instance_name",
        |         "/XXXX/instance_name/backend/database",
        |         "/XXXX/instance_name/backend/discoveryserver",
        |         "/XXXX/instance_name/backend/mainserver"
        |       ]
        |     },
        |     {
        |        "id": "database-backup",
        |        "cpus": 0.1,
        |        "mem": 512,
        |        "instances": 1,
        |        "requirePorts": false,
        |        "args" : [],
        |        "container": {
        |          "type": "DOCKER",
        |          "volumes": [
        |            {
        |              "containerPath": "database_backup_volume",
        |              "mode": "RW",
        |              "persistent": {
        |                "size": 50000
        |              }
        |            },
        |            {
        |              "containerPath": "/tmp",
        |              "hostPath": "database_backup_volume",
        |              "mode": "RW"
        |            }
        |         ],
        |          "docker": {
        |            "image": "XXXXinc/postgres-backup-service:docker_tag",
        |            "network": "BRIDGE",
        |            "forcePullImage": true
        |          }
        |        },
        |        "env" : {
        |          "DATABASE_USERNAME"       : "spring_datasource_username",
        |          "PGPASSWORD"              : "spring_datasource_password",
        |          "DATABASE_HOSTNAME"       : "database.backend.instance_name.XXXX.marathon.mesos",
        |          "AZURE_STORAGE_ACCOUNT"   : "database_backup_azure_storage_account",
        |          "AZURE_STORAGE_CONTAINER" : "database_backup_azure_storage_container",
        |          "AZURE_SAS_TOKEN"         : "database_backup_azure_sas_token"
        |        },
        |        "fetch" : [
        |          {
        |             "uri" : "docker_credentials_url",
        |             "executable" : false,
        |             "extract" : true,
        |             "cache" : true
        |          }
        |        ],
        |        "dependencies": [
        |         "/XXXX/instance_name/backend/database"
        |        ],
        |        "upgradeStrategy" : {
        |          "maximumOverCapacity": 0,
        |          "minimumHealthCapacity" : 0
        |        }
        |      }
        |    ]
        |}""".stripMargin


    import mesosphere.marathon.core.plugin.PluginManager
    import mesosphere.marathon.state.AppDefinition
    import play.api.libs.json.Json

    val app: AppDefinition = Json.parse(appDefinition).as[AppDefinition]

    // validate
    val validAppDefinition = AppDefinition.validAppDefinition(Set.empty[String])(PluginManager.None)
    val result = validAppDefinition(app)

    // print result
    println(result)
  }
}
