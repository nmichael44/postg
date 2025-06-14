ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.7.1"

val postgresVersion = "42.7.7"
val log4catsSlf4jVersion = "2.7.1"
val logbackVersion = "1.5.18"
val doobieVersion = "1.0.0-RC9"
val http4sVersion = "0.23.30"
val circeVersion = "0.14.14"
val scalatestVersion = "3.2.19"
val pureConfigCoreVersion = "0.17.9"
val catsEffectTestingScalatestVersion = "1.6.0"
val jwtCirceVersion = "10.0.4"
val password4jVersion = "1.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "postg",
    scalacOptions ++= Seq("-deprecation", "-Xmax-inlines:64", "-language:strictEquality"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.6.1",
      "org.postgresql" % "postgresql" % postgresVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion,
      "org.tpolecat" %% "doobie-specs2" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-literal" % circeVersion,
      "com.github.pureconfig" %% "pureconfig-core" % pureConfigCoreVersion,
      "org.typelevel" %% "log4cats-slf4j" % log4catsSlf4jVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime,
      "com.github.jwt-scala" %% "jwt-circe" % jwtCirceVersion,
      "com.password4j" % "password4j" % password4jVersion,
      "org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingScalatestVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    ),
    assembly / mainClass := Some("app.Main"),
    assembly / assemblyJarName := "postg.jar",
    assembly / assemblyMergeStrategy := {
      case "module-info.class" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
  )
