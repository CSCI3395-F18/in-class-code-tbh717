updateOptions := updateOptions.value.withLatestSnapshots(false)

lazy val root = (project in file("."))
  .settings(
    name         := "BigDataF18",
    organization := "edu.trinity",
    scalaVersion := "2.11.12",
    version      := "0.1.0-SNAPSHOT",
		libraryDependencies += "edu.trinity" %% "swiftvis2" % "0.1.0-SNAPSHOT",
		libraryDependencies += "edu.trinity" %% "swiftvis2spark" % "0.1.0-SNAPSHOT",
        libraryDependencies += "com.google.guava" % "guava" % "15.0",
        libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2",
        libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.2",
        libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.2",
        libraryDependencies += "commons-io" % "commons-io" % "2.4"
//		libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "0.9.1",
//		libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "0.9.1",
//		libraryDependencies += "org.deeplearning4j" % "dl4j-spark_2.11" % "0.9.1_spark_2",
//		libraryDependencies += "org.datavec" % "datavec-api" % "0.9.1",
//
//		libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1",
//		libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
//		libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1",
//		libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.3.1"
//		libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.3.1" % "provided"
  )
