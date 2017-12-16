package mesosphere.marathon
package api.akkahttp.v2

import java.net.InetAddress

import akka.event.EventStream
import akka.http.scaladsl.model.Uri.{ Path, Query }
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ Location, `Remote-Address` }
import mesosphere.{ UnitTest, ValidationTestLike }
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import mesosphere.marathon.api.TestAuthFixture
import mesosphere.marathon.api.akkahttp.EntityMarshallers.ValidationFailed
import mesosphere.marathon.api.akkahttp.{ Headers, Rejections }
import mesosphere.marathon.api.akkahttp.Rejections.{ EntityNotFound, Message }
import mesosphere.marathon.api.v2.validation.NetworkValidationMessages
import mesosphere.marathon.core.appinfo.PodStatusService
import mesosphere.marathon.core.deployment.DeploymentPlan
import mesosphere.marathon.core.election.ElectionService
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.pod.{ PodDefinition, PodManager }
import mesosphere.marathon.state.PathId
import mesosphere.marathon.core.pod.PodManager
import mesosphere.marathon.raml.FixedPodScalingPolicy
import mesosphere.marathon.state.PathId
import mesosphere.marathon.test.SettableClock
import mesosphere.marathon.util.SemanticVersion
import play.api.libs.json._
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration._

class PodsControllerTest extends UnitTest with ScalatestRouteTest with RouteBehaviours with ValidationTestLike with ResponseMatchers {

  "PodsController" should {
    "support pods" in {
      val controller = Fixture().controller()
      Head(Uri./) ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        responseAs[String] shouldBe empty
      }
    }

    // Unauthenticated access test cases
    {
      val controller = Fixture(authenticated = false).controller()
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Head(Uri./))
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Post(Uri./))
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Get("/::status"))
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Delete("/mypod"))
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Get("/mypod"))
      behave like unauthenticatedRoute(forRoute = controller.route, withRequest = Get("/mypod::status"))
    }

    // Unauthorized access test cases
    {
      val f = Fixture(authorized = false)
      val controller = f.controller()
      val podSpecJson = """
                          | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers": [
                          |   { "name": "webapp",
                          |     "resources": { "cpus": 0.03, "mem": 64 },
                          |     "image": { "kind": "DOCKER", "id": "busybox" },
                          |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                        """.stripMargin
      val entity = HttpEntity(podSpecJson).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      val podDefinition = PodDefinition(id = PathId("mypod"))
      f.podManager.find(any).returns(Some(podDefinition))

      behave like unauthorizedRoute(forRoute = controller.route, withRequest = request)
      behave like unauthorizedRoute(forRoute = controller.route, withRequest = Delete("/mypod"))
      behave like unauthorizedRoute(forRoute = controller.route, withRequest = Get("/mypod"))
      behave like unauthorizedRoute(forRoute = controller.route, withRequest = Get("/mypod::status"))
    }

    // Entity not found test cases
    {
      val f = Fixture()
      val controller = f.controller()
      val mypodId = PathId("mypod")
      val podDefinition = PodDefinition(id = mypodId)

      f.podManager.find(eq(PathId("unknown-pod"))).returns(None)
      f.podManager.find(eq(mypodId)).returns(Some(podDefinition))

      f.podManager.version(any, any).returns(Future.successful(None))
      def unknownPod(withRequest: HttpRequest, withMessage: String) = unknownEntity(controller.route, withRequest, withMessage)

      behave like unknownPod(withRequest = Delete("/unknown-pod"), withMessage = "Pod 'unknown-pod' does not exist")
      behave like unknownPod(withRequest = Get("/unknown-pod"), withMessage = "Pod 'unknown-pod' does not exist")
      behave like unknownPod(withRequest = Get("/unknown-pod::versions"), withMessage = "Pod 'unknown-pod' does not exist")
      behave like unknownPod(withRequest = Get("/mypod::versions/2015-04-09T12:30:00.000Z"), withMessage = "Pod 'mypod' does not exist in version 2015-04-09T12:30:00.000Z")
      behave like unknownPod(withRequest = Get("/mypod::versions/unparsable-datetime"), withMessage = "Pod 'mypod' does not exist in version unparsable-datetime")
    }

    "be able to create a simple single-container pod from docker image w/ shell command" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah")) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJson = """
                          | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers": [
                          |   { "name": "webapp",
                          |     "resources": { "cpus": 0.03, "mem": 64 },
                          |     "image": { "kind": "DOCKER", "id": "busybox" },
                          |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                        """.stripMargin
      val entity = HttpEntity(podSpecJson).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)
        response.header[Location].value.value() should be("/mypod")

        val jsonResponse: JsValue = Json.parse(responseAs[String])

        jsonResponse should have (
          executorResources(cpus = 0.1, mem = 32.0, disk = 10.0),
          noDefinedNetworkname,
          networkMode(raml.NetworkMode.Host)
        )
      }
    }

    "be able to create a simple single-container pod with bridge network" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah"))
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithBridgeNetwork = """
                                           | { "id": "/mypod", "networks": [ { "mode": "container/bridge" } ], "containers": [
                                           |   { "name": "webapp",
                                           |     "resources": { "cpus": 0.03, "mem": 64 },
                                           |     "image": { "kind": "DOCKER", "id": "busybox" },
                                           |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                                         """.stripMargin
      val entity = HttpEntity(podSpecJsonWithBridgeNetwork).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)
        response.header[Location].value.value() should be("/mypod")

        val jsonResponse = Json.parse(responseAs[String])

        jsonResponse should have (
          executorResources (cpus = 0.1, mem = 32.0, disk = 10.0),
          noDefinedNetworkname,
          networkMode(raml.NetworkMode.ContainerBridge)
        )
      }
    }

    "The secrets feature is NOT enabled and create pod (that uses file base secrets) fails" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah")) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithFileBasedSecret = """
                                             | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers":
                                             |   [
                                             |     { "name": "webapp",
                                             |       "resources": { "cpus": 0.03, "mem": 64 },
                                             |       "image": { "kind": "DOCKER", "id": "busybox" },
                                             |       "exec": { "command": { "shell": "sleep 1" } },
                                             |       "volumeMounts": [ { "name": "vol", "mountPath": "mnt2" } ]
                                             |     }
                                             |   ],
                                             |   "volumes": [ { "name": "vol", "secret": "secret1" } ],
                                             |   "secrets": { "secret1": { "source": "/path/to/my/secret" } }
                                             |  }
                                           """.stripMargin
      val entity = HttpEntity(podSpecJsonWithFileBasedSecret).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        rejection shouldBe a[ValidationFailed]
        inside(rejection) {
          case ValidationFailed(failure) =>
            failure should haveViolations("/podSecretVolumes(pod)" -> "Feature secrets is not enabled. Enable with --enable_features secrets)")
        }
      }
    }

    "The secrets feature is NOT enabled and create pod (that uses env secret refs) fails" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah")) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithEnvRefSecret = """
                                          | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers":
                                          |   [
                                          |     { "name": "webapp",
                                          |       "resources": { "cpus": 0.03, "mem": 64 },
                                          |       "image": { "kind": "DOCKER", "id": "busybox" },
                                          |       "exec": { "command": { "shell": "sleep 1" } }
                                          |     }
                                          |   ],
                                          |   "environment": { "vol": { "secret": "secret1" } },
                                          |   "secrets": { "secret1": { "source": "/foo" } }
                                          |  }
                                        """.stripMargin
      val entity = HttpEntity(podSpecJsonWithEnvRefSecret).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        rejection shouldBe a[ValidationFailed]
        inside(rejection) {
          case ValidationFailed(failure) =>
            failure should haveViolations("/secrets" -> "Feature secrets is not enabled. Enable with --enable_features secrets)")
        }
      }
    }

    "The secrets feature is NOT enabled and create pod (that uses env secret refs on container level) fails" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah")) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithEnvRefSecretOnContainerLevel = """
                                                          | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers":
                                                          |   [
                                                          |     { "name": "webapp",
                                                          |       "resources": { "cpus": 0.03, "mem": 64 },
                                                          |       "image": { "kind": "DOCKER", "id": "busybox" },
                                                          |       "exec": { "command": { "shell": "sleep 1" } },
                                                          |       "environment": { "vol": { "secret": "secret1" } }
                                                          |     }
                                                          |   ],
                                                          |   "secrets": { "secret1": { "source": "/path/to/my/secret" } }
                                                          |  }
                                                        """.stripMargin
      val entity = HttpEntity(podSpecJsonWithEnvRefSecretOnContainerLevel).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        rejection shouldBe a[ValidationFailed]
        inside(rejection) {
          case ValidationFailed(failure) =>
            failure should haveViolations("/secrets" -> "Feature secrets is not enabled. Enable with --enable_features secrets)")
        }
      }
    }

    "The secrets feature is enabled and create pod (that uses env secret refs on container level) succeeds" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah", "--enable_features", Features.SECRETS)) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithEnvRefSecretOnContainerLevel = """
                                                          | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers":
                                                          |   [
                                                          |     { "name": "webapp",
                                                          |       "resources": { "cpus": 0.03, "mem": 64 },
                                                          |       "image": { "kind": "DOCKER", "id": "busybox" },
                                                          |       "exec": { "command": { "shell": "sleep 1" } },
                                                          |       "environment": { "vol": { "secret": "secret1" } }
                                                          |     }
                                                          |   ],
                                                          |   "secrets": { "secret1": { "source": "/path/to/my/secret" } }
                                                          |  }
                                                        """.stripMargin
      val entity = HttpEntity(podSpecJsonWithEnvRefSecretOnContainerLevel).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)

        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have (podContainerWithEnvSecret("secret1"))
      }
    }

    "The secrets feature is enabled and create pod (that uses file based secrets) succeeds" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah", "--enable_features", Features.SECRETS)) // should not be injected into host network spec
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithFileBasedSecret = """
                                             | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers":
                                             |   [
                                             |     { "name": "webapp",
                                             |       "resources": { "cpus": 0.03, "mem": 64 },
                                             |       "image": { "kind": "DOCKER", "id": "busybox" },
                                             |       "exec": { "command": { "shell": "sleep 1" } },
                                             |       "volumeMounts": [ { "name": "vol", "mountPath": "mnt2" } ]
                                             |     }
                                             |   ],
                                             |   "volumes": [ { "name": "vol", "secret": "secret1" } ],
                                             |   "secrets": { "secret1": { "source": "/path/to/my/secret" } }
                                             |  }
                                           """.stripMargin
      val entity = HttpEntity(podSpecJsonWithFileBasedSecret).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)

        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have (podWithFileBasedSecret ("secret1"))
      }
    }

    "create a pod w/ container networking" in {
      val f = Fixture(configArgs = Seq("--default_network_name", "blah")) // required since network name is missing from JSON
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithContainerNetworking = """
                                                 | { "id": "/mypod", "networks": [ { "mode": "container" } ], "containers": [
                                                 |   { "name": "webapp",
                                                 |     "resources": { "cpus": 0.03, "mem": 64 },
                                                 |     "image": { "kind": "DOCKER", "id": "busybox" },
                                                 |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                                               """.stripMargin
      val entity = HttpEntity(podSpecJsonWithContainerNetworking).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)
        response.header[Location].value.value() should be("/mypod")

        val jsonResponse = Json.parse(responseAs[String])

        jsonResponse should have(
          executorResources(cpus = 0.1, mem = 32.0, disk = 10.0),
          definedNetworkName("blah"),
          networkMode(raml.NetworkMode.Container)
        )
      }
    }

    "create a pod w/ container networking w/o default network name" in {
      val f = Fixture()
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithContainerNetworking = """
                                                 | { "id": "/mypod", "networks": [ { "mode": "container" } ], "containers": [
                                                 |   { "name": "webapp",
                                                 |     "resources": { "cpus": 0.03, "mem": 64 },
                                                 |     "image": { "kind": "DOCKER", "id": "busybox" },
                                                 |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                                               """.stripMargin
      val entity = HttpEntity(podSpecJsonWithContainerNetworking).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        rejection shouldBe a[ValidationFailed]
        inside(rejection) {
          case ValidationFailed(failure) =>
            failure should haveViolations("/networks" -> NetworkValidationMessages.NetworkNameMustBeSpecified)
        }
      }
    }

    "create a pod with custom executor resource declaration" in {
      val f = Fixture()
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.create(any, eq(false)).returns(Future.successful(deploymentPlan))

      val podSpecJsonWithExecutorResources = """
                                               | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers": [
                                               |   { "name": "webapp",
                                               |     "resources": { "cpus": 0.03, "mem": 64 },
                                               |     "image": { "kind": "DOCKER", "id": "busybox" },
                                               |     "exec": { "command": { "shell": "sleep 1" } } } ],
                                               |     "executorResources": { "cpus": 100, "mem": 100 } }
                                             """.stripMargin
      val entity = HttpEntity(podSpecJsonWithExecutorResources).withContentType(ContentTypes.`application/json`)
      val request = Post(Uri./.withQuery(Query("force" -> "false")))
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.Created)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)
        response.header[Location].value.value() should be("/mypod")

        val jsonResponse = Json.parse(responseAs[String])

        jsonResponse should have(executorResources(cpus = 100.0, mem = 100.0, disk = 10.0))
      }
    }

    "delete a pod" in {
      val f = Fixture()
      val controller = f.controller()

      val pod = PodDefinition(id = PathId("mypod"))
      f.podManager.find(eq(PathId("mypod"))).returns(Some(pod))

      val plan = DeploymentPlan.empty
      f.podManager.delete(any, eq(false)).returns(Future.successful(plan))

      Delete("/mypod") ~> controller.route ~> check {
        response.status should be(StatusCodes.Accepted)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(plan.id)
      }
    }

    "respond with a pod for a lookup" in {
      val f = Fixture()
      val controller = f.controller()

      val podDefinition = PodDefinition(id = PathId("mypod"))
      f.podManager.find(eq(PathId("mypod"))).returns(Some(podDefinition))

      Get("/mypod") ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have(podId("mypod"))
      }
    }

    "respond with status for a pod" in {
      val f = Fixture()
      val controller = f.controller()

      val pod = PodDefinition(id = PathId("an-awesome-group/mypod"))
      f.podManager.find(eq(PathId("an-awesome-group/mypod"))).returns(Some(pod))

      val podStatus = raml.PodStatus(
        id = "an-awesome-group/mypod",
        spec = raml.Pod(id = "an-awesome-group/mypod", containers = Seq.empty),
        status = raml.PodState.Stable,
        statusSince = f.clock.now().toOffsetDateTime,
        lastUpdated = f.clock.now().toOffsetDateTime,
        lastChanged = f.clock.now().toOffsetDateTime
      )
      f.podStatusService.selectPodStatus(eq(PathId("an-awesome-group/mypod")), any).returns(Future.successful(Some(podStatus)))

      Get("/an-awesome-group/mypod::status") ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)

        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have(
          podId("an-awesome-group/mypod"),
          podState(raml.PodState.Stable)
        )
      }
    }

    "respond with all pods" in {
      val f = Fixture()
      val controller = f.controller()

      val podDefinitions = Seq(PodDefinition(id = PathId("mypod")), PodDefinition(id = PathId("another_pod")))
      f.podManager.findAll(any).returns(podDefinitions)

      Get(Uri./) ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse shouldBe a[JsArray]
        (jsonResponse \ 0).get should have(podId("mypod"))
        (jsonResponse \ 1).get should have(podId("another_pod"))
      }
    }

    "response with statuses for all pods" in {
      val f = Fixture()
      val controller = f.controller()

      val podStatus0 = raml.PodStatus(
        id = "mypod",
        spec = raml.Pod(id = "mypod", containers = Seq.empty),
        status = raml.PodState.Stable,
        statusSince = f.clock.now().toOffsetDateTime,
        lastUpdated = f.clock.now().toOffsetDateTime,
        lastChanged = f.clock.now().toOffsetDateTime
      )
      f.podStatusService.selectPodStatus(eq(PathId("mypod")), any).returns(Future.successful(Some(podStatus0)))

      val podStatus1 = raml.PodStatus(
        id = "another-pod",
        spec = raml.Pod(id = "another-pod", containers = Seq.empty),
        status = raml.PodState.Degraded,
        statusSince = f.clock.now().toOffsetDateTime,
        lastUpdated = f.clock.now().toOffsetDateTime,
        lastChanged = f.clock.now().toOffsetDateTime
      )
      f.podStatusService.selectPodStatus(eq(PathId("another-pod")), any).returns(Future.successful(Some(podStatus1)))
      f.podManager.ids().returns(Set(PathId("mypod"), PathId("another-pod")))

      Get("/::status") ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)

        val jsonResponse = Json.parse(responseAs[String])
        (jsonResponse \ 0).get should have(
          podId("mypod"),
          podState(raml.PodState.Stable)
        )
        (jsonResponse \ 1).get should have(
          podId("another-pod"),
          podState(raml.PodState.Degraded)
        )
      }
    }

    "respond with all available versions" in {
      val f = Fixture()
      val controller = f.controller()

      val pod = PodDefinition(id = PathId("mypod"))
      f.podManager.find(eq(PathId("mypod"))).returns(Some(pod))

      val versions = Seq(f.clock.now(), f.clock.now() + 1.minute)
      f.podManager.versions(eq(PathId("mypod"))).returns(Source(versions))

      Get("/mypod::versions") ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        val jsonResponse = Json.parse(responseAs[String])
        (jsonResponse \ 0).get.asOpt[String] should be(Some("2015-04-09T12:30:00.000Z"))
        (jsonResponse \ 1).get.asOpt[String] should be(Some("2015-04-09T12:31:00.000Z"))
      }
    }

    "respond with a specific version" in {
      val f = Fixture()
      val controller = f.controller()

      val pod = PodDefinition(id = PathId("mypod"))
      val version = f.clock.now()
      f.podManager.version(eq(PathId("mypod")), eq(version)).returns(Future.successful(Some(pod)))

      Get("/mypod::versions/2015-04-09T12:30:00.000Z") ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have(podId("mypod"))
      }
    }

    "update a simple single-container pod from docker image w/ shell command" in {
      implicit val podSystem = mock[PodManager]
      val f = Fixture()
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.update(any, eq(false)).returns(Future.successful(deploymentPlan))

      val postJson = """
                       | { "id": "/mypod", "networks": [ { "mode": "host" } ], "containers": [
                       |   { "name": "webapp",
                       |     "resources": { "cpus": 0.03, "mem": 64 },
                       |     "image": { "kind": "DOCKER", "id": "busybox" },
                       |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                     """.stripMargin
      val entity = HttpEntity(postJson).withContentType(ContentTypes.`application/json`)
      val request = Put("/mypod")
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)
        val jsonResponse = Json.parse(responseAs[String])
        jsonResponse should have(
          podId("/mypod"),
          executorResources(cpus = 0.1, mem = 32.0, disk = 10.0),
          noDefinedNetworkname,
          networkMode(raml.NetworkMode.Host)
        )
      }
    }

    "do not update if we have concurrent change error" in {
      implicit val podSystem = mock[PodManager]
      val f = Fixture()
      val controller = f.controller()

      val podId = PathId("/unknownpod")

      f.podManager.update(any, eq(false)).returns(Future.failed(ConflictingChangeException("pod is already there")))

      val postJson = """
                       | { "id": "/unknownpod", "networks": [ { "mode": "host" } ], "containers": [
                       |   { "name": "webapp",
                       |     "resources": { "cpus": 0.03, "mem": 64 },
                       |     "image": { "kind": "DOCKER", "id": "busybox" },
                       |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                     """.stripMargin
      val entity = HttpEntity(postJson).withContentType(ContentTypes.`application/json`)
      val request = Put(podId.toString)
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        rejection shouldBe a[Rejections.ConflictingChange]
        inside(rejection) {
          case r: Rejections.ConflictingChange => r.message.message shouldEqual "pod is already there"
        }
      }
    }

    "save pod with more than one instance" in {
      val f = Fixture()
      val controller = f.controller()

      val deploymentPlan = DeploymentPlan.empty
      f.podManager.update(any, eq(false)).returns(Future.successful(deploymentPlan))

      val postJson = """
                       | { "id": "/mypod", "networks": [ { "mode": "host" } ],
                       | "scaling": { "kind": "fixed", "instances": 2 }, "containers": [
                       |   { "name": "webapp",
                       |     "resources": { "cpus": 0.03, "mem": 64 },
                       |     "exec": { "command": { "shell": "sleep 1" } } } ] }
                     """.stripMargin

      val entity = HttpEntity(postJson).withContentType(ContentTypes.`application/json`)
      val request = Put("/mypod")
        .withEntity(entity)
        .withHeaders(`Remote-Address`(RemoteAddress(InetAddress.getByName("192.168.3.12"))))

      request ~> controller.route ~> check {
        response.status should be(StatusCodes.OK)
        response.header[Headers.`Marathon-Deployment-Id`].value.value() should be(deploymentPlan.id)

        val jsonResponse = Json.parse(responseAs[String])

        jsonResponse should have(
          scalingPolicyInstances(2)
        )
      }
    }
  }

  case class Fixture(
      configArgs: Seq[String] = Seq.empty[String],
      authenticated: Boolean = true,
      authorized: Boolean = true,
      isLeader: Boolean = true) {
    val config = AllConf.withTestConfig(configArgs: _*)
    val clock = new SettableClock

    val auth = new TestAuthFixture()
    auth.authenticated = authenticated
    auth.authorized = authorized

    val electionService = mock[ElectionService]
    val groupManager = mock[GroupManager]
    val podManager = mock[PodManager]
    val podStatusService = mock[PodStatusService]
    val pluginManager = PluginManager.None
    val eventBus = mock[EventStream]
    val scheduler = mock[MarathonScheduler]

    electionService.isLeader returns (isLeader)
    scheduler.mesosMasterVersion() returns Some(SemanticVersion(0, 0, 0))

    implicit val authenticator = auth.auth
    implicit val mat: Materializer = mock[Materializer]
    def controller() = new PodsController(config, electionService, podManager, podStatusService, groupManager, pluginManager, eventBus, scheduler, clock)
  }
}
