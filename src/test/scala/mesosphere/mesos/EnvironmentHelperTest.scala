package mesosphere.mesos

import mesosphere.UnitTest

import scala.collection.immutable.Seq

class EnvironmentHelperTest extends UnitTest {
  "The EnvironmentHelper" must {
    "PortsEnv" in {
      val env = EnvironmentHelper.portsEnv(Seq(None -> 0, None -> 0), Helpers.hostPorts(1001, 1002))
      assert("1001" == env("PORT"))
      assert("1001" == env("PORT0"))
      assert("1002" == env("PORT1"))
      assert(!env.contains("PORT_0"))
    }

    "PortsEnvEmpty" in {
      val env = EnvironmentHelper.portsEnv(Nil, Nil)
      assert(Map.empty == env) // linter:ignore:UnlikelyEquality
    }

    "PortsNamedEnv" in {
      val env = EnvironmentHelper.portsEnv(Seq(Some("http") -> 0, Some("https") -> 0), Helpers.hostPorts(1001, 1002))
      assert("1001" == env("PORT"))
      assert("1001" == env("PORT0"))
      assert("1002" == env("PORT1"))

      assert("1001" == env("PORT_HTTP"))
      assert("1002" == env("PORT_HTTPS"))
    }

    "DeclaredPortsEnv" in {
      val env = EnvironmentHelper.portsEnv(Seq(None -> 80, None -> 8080), Helpers.hostPorts(1001, 1002))
      assert("1001" == env("PORT"))
      assert("1001" == env("PORT0"))
      assert("1002" == env("PORT1"))

      assert("1001" == env("PORT_80"))
      assert("1002" == env("PORT_8080"))
    }

    "DeclaredPortsEnvNamed" in {
      val env = EnvironmentHelper.portsEnv(Seq(Some("http") -> 80, None -> 8080, Some("https") -> 443), Helpers.hostPorts(1001, 1002, 1003))
      assert("1001" == env("PORT"))
      assert("1001" == env("PORT0"))
      assert("1002" == env("PORT1"))
      assert("1003" == env("PORT2"))

      assert("1001" == env("PORT_80"))
      assert("1002" == env("PORT_8080"))
      assert("1003" == env("PORT_443"))

      assert("1001" == env("PORT_HTTP"))
      assert("1003" == env("PORT_HTTPS"))
    }
  }

  object Helpers {
    def hostPorts(p: Int*): Seq[Option[Int]] = collection.immutable.Seq(p: _*).map(Some(_))
  }
}
