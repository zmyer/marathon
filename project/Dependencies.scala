import sbt.ExclusionRule
import sbt._

object Dependencies {
  import Dependency._

  val pluginInterface = Seq(
    playJson % "compile",
    mesos % "compile",
    guava % "compile",
    wixAccord % "compile",
    scalaxml % "provided" // for scapegoat
  )

  val excludeSlf4jLog4j12 = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  val excludeLog4j = ExclusionRule(organization = "log4j")
  val excludeJCL = ExclusionRule(organization = "commons-logging")

  val marathon = (Seq(
    // runtime
    asyncAwait % "compile",
    sprayClient % "compile",
    sprayHttpx % "compile",
    mesos % "compile",
    jodaTime % "compile",
    jodaConvert % "compile",
    uuidGenerator % "compile",
    jGraphT % "compile",
    hadoopHdfs % "compile",
    hadoopCommon % "compile",
    beanUtils % "compile",
    playJson % "compile",
    jsonSchemaValidator % "compile",
    rxScala % "compile",
    marathonUI % "compile",
    graphite % "compile",
    datadog % "compile",
    marathonApiConsole % "compile",
    wixAccord % "compile",
    curator % "compile",
    curatorClient % "compile",
    curatorFramework % "compile",
    java8Compat % "compile",
    scalaLogging % "compile",
    logstash % "compile",
    raven % "compile",

    // test
    Test.diffson % "test",
    Test.scalatest % "test",
    Test.mockito % "test",
    Test.junit % "test",
    Test.scalacheck % "test",
    Test.wixAccordScalatest % "test",
    Test.curatorTest
  ) ++ Akka.all).map(_.excludeAll(excludeSlf4jLog4j12).excludeAll(excludeLog4j).excludeAll(excludeJCL))

  val benchmark = Seq(
    Test.jmh
  )
}

object Dependency {
  object V {
    // runtime deps versions
    val Guava = "19.0"
    // FIXME (gkleiman): reenable deprecation checks after Mesos 1.0.0-rc2 deprecations are handled
    val Mesos = "1.1.0"
    // Version of Mesos to use in Dockerfile.
    val MesosDebian = "1.1.0-2.0.107.debian81"

    val AsyncAwait = "0.9.6"
    val Spray = "1.3.4"
    val TwitterCommons = "0.0.76"
    val Jersey = "2.25"
    val Jetty = "9.4.0.v20161208"
    val JodaTime = "2.9.6"
    val JodaConvert = "1.8.1"
    val UUIDGenerator = "3.1.4"
    val JGraphT = "0.9.3"
    val Hadoop = "2.7.2"
    val Diffson = "2.0.2"
    val PlayJson = "2.5.10"
    val JsonSchemaValidator = "2.2.6"
    val RxScala = "0.26.4"
    val MarathonUI = "1.1.6"
    val MarathonApiConsole = "3.0.8"
    val Graphite = "3.1.2"
    val DataDog = "1.1.6"
    val Logback = "1.1.3"
    val Logstash = "4.8"
    val WixAccord = "0.5"
    val Curator = "2.11.1"
    val Java8Compat = "0.8.0"
    val ScalaLogging = "3.5.0"
    val Raven = "7.8.0"

    // test deps versions
    val Mockito = "1.10.19"
    val ScalaTest = "3.0.1"
    val JUnit = "4.12"
    val JUnitBenchmarks = "0.7.2"
    val JMH = "1.14"
    val ScalaCheck = "1.13.4"
  }

  val excludeMortbayJetty = ExclusionRule(organization = "org.mortbay.jetty")
  val excludeJavaxServlet = ExclusionRule(organization = "javax.servlet")

  object Akka {
    val Version = "2.4.16"
    val HttpVersion = "10.0.0"

    val actor = "com.typesafe.akka" %% "akka-actor" % Version
    val slf4j = "com.typesafe.akka" %% "akka-slf4j" % Version
    val stream = "com.typesafe.akka" %% "akka-stream" % Version
    val http = "com.typesafe.akka" %% "akka-http" % HttpVersion
    val httpPlayJson = "de.heikoseeberger" %% "akka-http-play-json" % "1.10.1"
    val httpSse = "de.heikoseeberger" %% "akka-sse" % "2.0.0-RC5"
    val httpCors = "ch.megard" %% "akka-http-cors" % "0.1.10"

    val testKit = "com.typesafe.akka" %% "akka-testkit" % Version

    val all = Seq(
      actor % "compile",
      slf4j % "compile",
      stream % "compile",
      http % "compile",
      httpPlayJson % "compile",
      httpSse % "compile",
      httpCors % "compile",
      testKit % "test"
    )
  }



  val asyncAwait = "org.scala-lang.modules" %% "scala-async" % V.AsyncAwait
  val sprayClient = "io.spray" %% "spray-client" % V.Spray
  val sprayHttpx = "io.spray" %% "spray-httpx" % V.Spray
  val playJson = "com.typesafe.play" %% "play-json" % V.PlayJson
  val guava = "com.google.guava" % "guava" % V.Guava
  val mesos = "org.apache.mesos" % "mesos" % V.Mesos
  val jodaTime = "joda-time" % "joda-time" % V.JodaTime
  val jodaConvert = "org.joda" % "joda-convert" % V.JodaConvert
  val uuidGenerator = "com.fasterxml.uuid" % "java-uuid-generator" % V.UUIDGenerator
  val jGraphT = "org.javabits.jgrapht" % "jgrapht-core" % V.JGraphT
  val hadoopHdfs = "org.apache.hadoop" % "hadoop-hdfs" % V.Hadoop excludeAll(excludeMortbayJetty, excludeJavaxServlet)
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % V.Hadoop excludeAll(excludeMortbayJetty,
    excludeJavaxServlet)
  val beanUtils = "commons-beanutils" % "commons-beanutils" % "1.9.3"
  val jsonSchemaValidator = "com.github.fge" % "json-schema-validator" % V.JsonSchemaValidator
  val rxScala = "io.reactivex" %% "rxscala" % V.RxScala
  val marathonUI = "mesosphere.marathon" % "ui" % V.MarathonUI
  val marathonApiConsole = "mesosphere.marathon" % "api-console" % V.MarathonApiConsole
  val graphite = "io.dropwizard.metrics" % "metrics-graphite" % V.Graphite
  val datadog = "org.coursera" % "dropwizard-metrics-datadog" % V.DataDog exclude("ch.qos.logback", "logback-classic")
  val logstash = "net.logstash.logback" % "logstash-logback-encoder" % V.Logstash
  val wixAccord = "com.wix" %% "accord-core" % V.WixAccord
  val curator = "org.apache.curator" % "curator-recipes" % V.Curator
  val curatorClient = "org.apache.curator" % "curator-client" % V.Curator
  val curatorFramework = "org.apache.curator" % "curator-framework" % V.Curator
  val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % V.Java8Compat
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % V.ScalaLogging
  val scalaxml = "org.scala-lang.modules" %% "scala-xml" % "1.0.5"
  val raven = "com.getsentry.raven" % "raven-logback" % V.Raven

  object Test {
    val jmh = "org.openjdk.jmh" % "jmh-generator-annprocess" % V.JMH
    val scalatest = "org.scalatest" %% "scalatest" % V.ScalaTest
    val mockito = "org.mockito" % "mockito-all" % V.Mockito
    val diffson = "org.gnieh" %% "diffson" % V.Diffson
    val junit = "junit" % "junit" % V.JUnit
    val scalacheck = "org.scalacheck" %% "scalacheck" % V.ScalaCheck
    val wixAccordScalatest = "com.wix" %% "accord-scalatest" % V.WixAccord
    val curatorTest = "org.apache.curator" % "curator-test" % V.Curator
  }
}
