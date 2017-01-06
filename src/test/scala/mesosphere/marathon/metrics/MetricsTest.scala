package mesosphere.marathon
package metrics

import com.google.inject.matcher.{ AbstractMatcher, Matchers }
import com.google.inject.{ AbstractModule, Guice }
import mesosphere.FunTest
import mesosphere.marathon.core.task.tracker.InstanceTracker
import org.aopalliance.intercept.{ MethodInterceptor, MethodInvocation }

class FooBar {
  def dummy(): Unit = {}
}

class MetricsTest extends FunTest {

  class TestModule extends AbstractModule {

    class DummyBehavior extends MethodInterceptor {
      override def invoke(invocation: MethodInvocation): AnyRef = {
        invocation.proceed()
      }
    }

    object MarathonMatcher extends AbstractMatcher[Class[_]] {
      override def matches(t: Class[_]): Boolean = t == classOf[FooBar]
    }

    override def configure(): Unit = {
      bindInterceptor(Matchers.any(), Matchers.any(), new DummyBehavior())
    }
  }

  test("Metrics#className should strip 'EnhancerByGuice' from the metric names") {
    val instance = Guice.createInjector(new TestModule).getInstance(classOf[FooBar])
    assert(instance.getClass.getName.contains("EnhancerByGuice"))

    assert(metrics.className(instance.getClass) == "mesosphere.marathon.metrics.FooBar")
  }

  test("Metrics#name should replace $ with .") {
    val instance = new Serializable {}
    assert(instance.getClass.getName.contains('$'))

    assert(metrics.name(ServiceMetric, instance.getClass, "test$method") ==
      s"${ServiceMetric.name}.mesosphere.marathon.metrics.MetricsTest.anonfun.2.anon.1.test.method")
  }

  test("Metrics#name should use a dot to separate the class name and the method name") {
    val expectedName = "service.mesosphere.marathon.core.task.tracker.InstanceTracker.write-request-time"
    val actualName = metrics.name(ServiceMetric, classOf[InstanceTracker], "write-request-time")

    assert(expectedName.equals(actualName))
  }
}
