sbt compile assembly && /home/jnguyenxuan/soft/spark-1.6.2/bin/spark-submit  --class "Neo4jConnector" --master local[4] target/scala-2.11/SciGraph-spark-assembly-1.0.jar

