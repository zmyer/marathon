package mesosphere.marathon
package core.event.impl.stream

import javax.servlet.http.HttpServletResponse

import akka.actor.ActorRef
import mesosphere.FunTest
import mesosphere.marathon.api.TestAuthFixture

class HttpEventStreamServletTest extends FunTest {

  test("access without authentication is denied") {
    Given("An unauthenticated request")
    val f = new Fixture
    val resource = f.streamServlet()
    val response = mock[HttpServletResponse]
    f.auth.authenticated = false

    When("we try to attach to the event stream")
    resource.doGet(f.auth.request, response)

    Then("we receive a NotAuthenticated response")
    verify(response).setStatus(f.auth.NotAuthenticatedStatus)
  }

  test("access without authorization is denied") {
    Given("An unauthorized request")
    val f = new Fixture
    val resource = f.streamServlet()
    val response = mock[HttpServletResponse]
    f.auth.authenticated = true
    f.auth.authorized = false

    When("we try to attach to the event stream")
    resource.doGet(f.auth.request, response)

    Then("we receive a Unauthorized response")
    verify(response).setStatus(f.auth.UnauthorizedStatus)
  }

  class Fixture {
    val actor = mock[ActorRef]
    val auth = new TestAuthFixture
    val config = AllConf.withTestConfig()
    def streamServlet() = new HttpEventStreamServlet(actor, config, auth.auth, auth.auth)
  }
}

