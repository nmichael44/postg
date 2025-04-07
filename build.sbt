ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.2"

lazy val postgresVersion = "42.7.5"
lazy val logbackVersion = "1.5.18"
lazy val doobieVersion = "1.0.0-RC8"
lazy val http4sVersion = "0.23.30"
lazy val circeVersion = "0.14.12"
lazy val scalatestVersion = "3.2.19"

lazy val root = (project in file("."))
  .settings(
    name := "postg",
  )

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  // Start with this one
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
  "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
  "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime,

  // And add any of these as needed
  // "org.tpolecat" %% "doobie-h2"        % "1.0.0-RC4",          // H2 driver 1.4.200 + type mappings.
  // "org.tpolecat" %% "doobie-scalatest" % "1.0.0-RC4" % "test"  // ScalaTest support for type-checking statements.

  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
)

//libraryDependencies += "org.typelevel" %% "cats-core" % "2.9.0"
//libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.7"
//libraryDependencies += "org.typelevel" %% "cats-mtl" % "1.5.0"
Compile / run / fork := false
