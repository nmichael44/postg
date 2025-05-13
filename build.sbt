ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val scalaCompilerVersion = "3.6.4"

ThisBuild / scalaVersion := scalaCompilerVersion

lazy val postgresVersion = "42.7.5"
lazy val logbackVersion = "1.5.18"
lazy val doobieVersion = "1.0.0-RC9"
lazy val http4sVersion = "0.23.30"
lazy val circeVersion = "0.14.13"
lazy val scalatestVersion = "3.2.18"
lazy val pureConfigCoreVersion = "0.17.9"
lazy val catsEffectTestingScalatestVersion = "1.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "postg",
  )

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion, // Postgres driver 42.6.0 + type mappings.
  "org.tpolecat" %% "doobie-specs2" % doobieVersion, // Specs2 support for typechecking statements.
  "org.tpolecat" %% "doobie-hikari" % doobieVersion, // HikariCP transactor.
  "org.http4s" %% "http4s-ember-client" % http4sVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-ember-server" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-literal" % circeVersion,
  "com.github.pureconfig" %% "pureconfig-core" % pureConfigCoreVersion,
  "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
  "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime,
  "org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingScalatestVersion % Test,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
)

Compile / run / fork := false
