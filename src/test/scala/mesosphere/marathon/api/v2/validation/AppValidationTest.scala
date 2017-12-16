package mesosphere.marathon
package api.v2.validation

import com.wix.accord.Validator
import mesosphere.marathon.api.v2.AppNormalization
import mesosphere.marathon.raml._
import mesosphere.{UnitTest, ValidationTestLike}
import org.scalatest.prop.TableDrivenPropertyChecks

class AppValidationTest extends UnitTest with ValidationTestLike with TableDrivenPropertyChecks {

  val config = AppNormalization.Configuration(None, "mesos-bridge-name")
  val configWithDefaultNetworkName = AppNormalization.Configuration(Some("defaultNetworkName"), "mesos-bridge-name")
  val basicValidator: Validator[App] = AppValidation.validateCanonicalAppAPI(Set.empty, () => config.defaultNetworkName)
  val withSecretsValidator: Validator[App] = AppValidation.validateCanonicalAppAPI(Set("secrets"), () => config.defaultNetworkName)
  val withDefaultNetworkNameValidator: Validator[App] = AppValidation.validateCanonicalAppAPI(Set.empty, () => configWithDefaultNetworkName.defaultNetworkName)

  "File based secrets validation" when {
    "file based secret is used when secret feature is not enabled" should {
      "fail" in {
        val app = App(id = "/app", cmd = Some("cmd"),
          container = Option(raml.Container(`type` = EngineType.Mesos, volumes = Seq(AppSecretVolume("/path", "bar"))))
        )
        val validation = basicValidator(app)
        validation.isFailure shouldBe true
        // Here and below: stringifying validation is admittedly not the best way but it's a nested Set(GroupViolation...) and not easy to test.
        validation.toString should include ("Feature secrets is not enabled. Enable with --enable_features secrets")
      }
    }

    "file based secret is used when corresponding secret is missing" should {
      "fail" in {
        val app = App(id = "/app", cmd = Some("cmd"),
          secrets = Map[String, SecretDef]("foo" -> SecretDef("/bar")),
          container = Option(raml.Container(`type` = EngineType.Mesos, volumes = Seq(AppSecretVolume("/path", "baz"))))
        )
        val validation = withSecretsValidator(app)
        validation.isFailure shouldBe true
        validation.toString should include ("volume.secret must refer to an existing secret")
      }
    }

    "a valid file based secret" should {
      "succeed" in {
        val app = App(id = "/app", cmd = Some("cmd"),
          secrets = Map[String, SecretDef]("foo" -> SecretDef("/bar")),
          container = Option(raml.Container(`type` = EngineType.Mesos, volumes = Seq(AppSecretVolume("/path", "foo"))))
        )
        withSecretsValidator(app) shouldBe (aSuccess)
      }
    }
  }

  "Docker image pull config validation" when {
    "pull config when the Mesos containerizer is used and the corresponding secret is provided" should {
      "be accepted" in {
        val app = App(
          id = "/foo",
          cmd = Some("bar"),
          container = Some(Container(
            `type` = EngineType.Mesos,
            docker = Some(DockerContainer(
              image = "xyz", pullConfig = Some(DockerPullConfig("aSecret")))))),
          secrets = Map("aSecret" -> SecretDef("/secret")))

        withSecretsValidator(app) shouldBe (aSuccess)
      }
    }

    "pull config when the Mesos containerizer is used and the corresponding secret is not provided" should {
      "fail" in {
        val app = App(
          id = "/foo",
          cmd = Some("bar"),
          container = Some(Container(
            `type` = EngineType.Mesos,
            docker = Some(DockerContainer(
              image = "xyz", pullConfig = Some(DockerPullConfig("aSecret")))))))

        withSecretsValidator(app) should haveViolations(
          "/container/docker/pullConfig" -> "pullConfig.secret must refer to an existing secret")
      }
    }

    "pull config when the Docker containerizer is used and the corresponding secret is provided" should {
      "fail" in {
        val app = App(
          id = "/foo",
          cmd = Some("bar"),
          container = Some(Container(
            `type` = EngineType.Docker,
            docker = Some(DockerContainer(
              image = "xyz", pullConfig = Some(DockerPullConfig("aSecret")))))),
          secrets = Map("aSecret" -> SecretDef("/secret")))

        withSecretsValidator(app) should haveViolations(
          "/container/docker/pullConfig" -> "pullConfig is not supported with Docker containerizer")
      }
    }

    "pull config when secrets feature is disabled" should {
      "fail" in {
        val app = App(
          id = "/foo",
          cmd = Some("bar"),
          container = Some(Container(
            `type` = EngineType.Mesos,
            docker = Some(DockerContainer(
              image = "xyz", pullConfig = Some(DockerPullConfig("aSecret")))))))

        basicValidator(app) should haveViolations(
          "/container/docker/pullConfig" -> "must be empty",
          "/container/docker/pullConfig" -> "Feature secrets is not enabled. Enable with --enable_features secrets)",
          "/container/docker/pullConfig" -> "pullConfig.secret must refer to an existing secret")
      }
    }
  }

  "network validation" when {
    def networkedApp(portMappings: Seq[ContainerPortMapping], networks: Seq[Network], docker: Boolean = false) = {
      App(
        id = "/foo",
        cmd = Some("bar"),
        networks = networks,
        container = Some(Container(
          portMappings = Some(portMappings),
          `type` = if (docker) EngineType.Docker else EngineType.Mesos,
          docker = if (docker) Some(DockerContainer(image = "foo")) else None
        )))
    }

    def containerNetworkedApp(portMappings: Seq[ContainerPortMapping], networkCount: Int = 1, docker: Boolean = false) =
      networkedApp(
        portMappings,
        networks = 1.to(networkCount).map { i => Network(mode = NetworkMode.Container, name = Some(i.toString)) },
        docker = docker)

    "multiple container networks are specified for an app" should {

      "require networkNames for hostPort to containerPort mapping" in {
        val badApp = containerNetworkedApp(
          Seq(ContainerPortMapping(hostPort = Option(0))), networkCount = 2)

        basicValidator(badApp) should haveViolations(
          "/container/portMappings(0)" -> AppValidationMessages.NetworkNameRequiredForMultipleContainerNetworks)
      }

      "limit docker containers to a single network" in {
        val app = containerNetworkedApp(
          Seq(ContainerPortMapping()), networkCount = 2, true)
        basicValidator(app) should haveViolations("/" -> AppValidationMessages.DockerEngineLimitedToSingleContainerNetwork)
      }

      "allow portMappings that don't declare hostPort nor networkNames" in {
        val app = containerNetworkedApp(
          Seq(ContainerPortMapping()), networkCount = 2)
        basicValidator(app) should be(aSuccess)
      }

      "allow portMappings that both declare a hostPort and a networkNames" in {
        val app = containerNetworkedApp(Seq(
          ContainerPortMapping(
            hostPort = Option(0),
            networkNames = List("1"))), networkCount = 2)
        basicValidator(app) should be(aSuccess)
      }
    }

    "single container network" should {

      "consider a valid portMapping with a name as valid" in {
        basicValidator(
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                hostPort = Some(80),
                containerPort = 80,
                networkNames = List("1"))))) should be(aSuccess)
      }

      "consider a container network without name to be invalid" in {
        val result = basicValidator(
          networkedApp(Seq.empty, networks = Seq(Network(mode = NetworkMode.Container)), false))
        result.isFailure shouldBe true
      }

      "consider a container network without name but with a default name from config valid" in {
        withDefaultNetworkNameValidator(
          networkedApp(Seq.empty, networks = Seq(Network(mode = NetworkMode.Container)), false)) shouldBe (aSuccess)
      }

      "consider a portMapping with a hostPort and two valid networkNames as invalid" in {
        val app = containerNetworkedApp(
          Seq(
            ContainerPortMapping(
              hostPort = Some(80),
              containerPort = 80,
              networkNames = List("1", "2"))),
          networkCount = 3)
        basicValidator(app) should haveViolations(
          "/container/portMappings(0)" -> AppValidationMessages.NetworkNameRequiredForMultipleContainerNetworks)
      }

      "consider a portMapping with no name as valid" in {
        basicValidator(
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                hostPort = Some(80),
                containerPort = 80,
                networkNames = Nil)))) should be(aSuccess)
      }

      "consider a portMapping without a hostport as valid" in {
        basicValidator(
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                hostPort = None)))) should be(aSuccess)
      }

      "consider portMapping with zero hostport as valid" in {
        basicValidator(
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                containerPort = 80,
                hostPort = Some(0))))) should be(aSuccess)
      }

      "consider portMapping with a non-matching network name as invalid" in {
        val app =
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                containerPort = 80,
                hostPort = Some(80),
                networkNames = List("undefined-network-name"))))
        basicValidator(app) should haveViolations(
          "/container/portMappings(0)/networkNames(0)" -> "is not one of (1)")
      }

      "consider portMapping without networkNames nor hostPort as valid" in {
        basicValidator(
          containerNetworkedApp(
            Seq(
              ContainerPortMapping(
                containerPort = 80,
                hostPort = None,
                networkNames = Nil)))) should be(aSuccess)
      }
    }

    "general port validation" in {
      val app =
        containerNetworkedApp(
          Seq(
            ContainerPortMapping(
              name = Some("name"),
              hostPort = Some(123)),
            ContainerPortMapping(
              name = Some("name"),
              hostPort = Some(123))))
      basicValidator(app) should haveViolations(
        "/container/portMappings" -> "Port names must be unique.")
    }

    "missing hostPort is allowed for bridge networking (so we can normalize it)" in {
      // This isn't _actually_ allowed; we expect that normalization will replace the None to a Some(0) before
      // converting to an AppDefinition, in order to support legacy API
      basicValidator(networkedApp(
        portMappings = Seq(ContainerPortMapping(
          containerPort = 8080,
          hostPort = None,
          servicePort = 0,
          name = Some("foo"))),
        networks = Seq(Network(mode = NetworkMode.ContainerBridge, name = None)))) should be(aSuccess)
    }

  }

  "health check validation" when {
    val allowedProtocols = Set(
      AppHealthCheckProtocol.MesosHttp,
      AppHealthCheckProtocol.MesosHttps,
      AppHealthCheckProtocol.MesosTcp)

    val conditions = Table (
      ("protocol", "isAllowed"),
      (AppHealthCheckProtocol.MesosHttp, true),
      (AppHealthCheckProtocol.MesosHttps, true),
      (AppHealthCheckProtocol.MesosTcp, true),
      (AppHealthCheckProtocol.Command, false),
      (AppHealthCheckProtocol.Http, false),
      (AppHealthCheckProtocol.Https, false),
      (AppHealthCheckProtocol.Tcp, false),
    )

    "is docker app" should {
      val dockerApp = App(
        id = "/foo",
        container = Some(Container(
          `type` = EngineType.Docker,
          docker = Some(DockerContainer(
            image = "xyz")))))

      forAll (conditions) { (protocol: AppHealthCheckProtocol, isAllowed) =>
        s"${if(isAllowed) "pass" else "fail"} validation when protocol is $protocol and IPv6 ip protocol is defined" in {

          val dockerAppWithHealthCheck = dockerApp.copy(healthChecks =
            Set(AppHealthCheck(
              protocol = protocol,
              port = Some(80),
              ipProtocol = IpProtocol.Ipv6)))

          if (isAllowed) {
            basicValidator(dockerAppWithHealthCheck) should be(aSuccess)
          } else {
            basicValidator(dockerAppWithHealthCheck) should haveViolations(
              "/healthChecks(0)" -> AppValidationMessages.HealthCheckIpProtocolLimitation)
          }
        }
      }

      "pass validation even when no ipProtocol specified when Mesos HTTP/HTTPS/TCP" in {
        allowedProtocols.foreach { protocol =>
          val dockerAppWithMesosHttpHealthCheck = dockerApp.copy(healthChecks =
            Set(AppHealthCheck(
              protocol = protocol,
              port = Some(80))))

          basicValidator(dockerAppWithMesosHttpHealthCheck) should be(aSuccess)
        }
      }
    }

    "is UCR app" should {
      val ucrApp = App(
        id = "/foo",
        container = Some(Container(
          `type` = EngineType.Mesos,
          docker = Some(DockerContainer(
            image = "xyz")))))

      "fail even for otherwise supported types" in {
        allowedProtocols.foreach { protocol =>
          val ucrAppWithHealthCheck = ucrApp.copy(healthChecks =
            Set(AppHealthCheck(
              protocol = protocol,
              port = Some(80),
              ipProtocol = IpProtocol.Ipv6)))

          basicValidator(ucrAppWithHealthCheck) should haveViolations(
            "/healthChecks(0)" -> AppValidationMessages.HealthCheckIpProtocolLimitation)
        }
      }

      "should pass always for IPv4" in {
        allowedProtocols.foreach { protocol =>
          val ucrAppWithHealthCheck = ucrApp.copy(healthChecks =
            Set(AppHealthCheck(
              protocol = protocol,
              port = Some(80),
              ipProtocol = IpProtocol.Ipv4)))

          basicValidator(ucrAppWithHealthCheck) should be(aSuccess)
        }
      }
    }
  }
}
