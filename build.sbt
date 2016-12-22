name := "SciGraph-spark"

version := "1.0"

scalaVersion := "2.11.8"

spName := "monarchinitiative/SciGraph-spark"

sparkVersion := "2.0.2"

sparkComponents ++= Seq("graphx", "sql")

//spDependencies += "neo4j-contrib/neo4j-spark-connector:1.0.0-RC1"

spDependencies += "neo4j-contrib/neo4j-spark-connector:2.0.0-M2"
