import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.neo4j.spark._
import org.apache.spark.sql._
import scala.collection.JavaConverters._
import org.neo4j.driver.v1.types.Node
import org.neo4j.driver.v1.types.Relationship
import org.apache.commons.math3.distribution.HypergeometricDistribution

object Neo4jConnector2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Neo4j connector")
    val sc = new SparkContext(conf)

    val neo = Neo4j(sc)
    
    val samples = Seq("http://purl.obolibrary.org/obo/OMIM_215045")
    
    val rdd = neo.cypher(s"""MATCH (d:disease)-[:subClassOf*0..]->(parent:disease)
      WHERE d.iri in ["${samples.mkString("\", \"")}"]
      WITH d, parent
      MATCH (parent)-[r:`http://purl.obolibrary.org/obo/RO_0002200`]->(p:Phenotype)
      RETURN d as source, p as target, r""").loadRowRdd

    val myRows = rdd.map(r => {
      val source: Node = r(0).asInstanceOf[Node]
      val target: Node = r(1).asInstanceOf[Node]
      val relationship: Relationship = r(2).asInstanceOf[Relationship]

      MyRow(toMyNode(source), toMyNode(target), toMyEdge(relationship))
    })
    myRows.cache()
    //myRows.saveAsObjectFile("/tmp/dataspark")

    println(myRows.count())

    val omim = myRows.filter(r => {
      if (r.source.properties.keySet.exists(_ == "iri")) {
        val iri = r.source.properties.get("iri").map(_.toString)
        samples.contains(iri.getOrElse(""))
      } else {
        false
      }
    })

    val omimPhenotypeIds = omim.map(r => {
      r.target
    }).collect

    val allPhenotypeIds = myRows.map(r => {
      r.target
    }).collect

    println("omim phenotype: " + omim.count())
    //    omim.map(r => {
    //      val phenotypeId = r(1).asInstanceOf[Node].id
    //      countForDisease.put(phenotypeId, 1 + countForDisease.get(phenotypeId).getOrElse(0))
    //    }).collect()

    val countForDisease = omim.groupBy(r => {
      r.target
    }).mapValues(_.size).map(identity)

    //    rdd.map(r => {
    //      val phenotypeId = r(1).asInstanceOf[Node].id
    //      countForAll.put(phenotypeId, 1 + countForAll.get(phenotypeId).getOrElse(0))
    //    }).collect()

    val countForAll = myRows.take(100000).groupBy(r => {
      r.target
    }).mapValues(_.size).map(identity)

    countForDisease.map(println)
    countForAll.map(println)

    // HypergeometricDistribution(int populationSize, int numberOfSuccesses, int sampleSize)
    val populationSize = countForAll.size
    val hyp = countForDisease.map(r => r match {
      case (node, count) => {
        val numberOfSuccesses = count
        val sampleSize = samples.size
        val hypergeometricDistribution = new HypergeometricDistribution(populationSize, countForAll.get(node).get.toInt, sampleSize)
        val p =
          hypergeometricDistribution.upperCumulativeProbability(count)
        (p, node)
      }
    })

    hyp.collect.map(println)

  }

  def toMyNode(node: Node): MyNode = {
    val properties = node.asMap().asScala.toMap
    MyNode(node.id(), properties.filter(tuple => tuple._1 == "iri" || tuple._1 == "label"))
  }

  def toMyEdge(edge: Relationship): MyEdge = {
    val properties = edge.asMap().asScala.toMap
    MyEdge(edge.id(), edge.startNodeId(), edge.endNodeId(), properties.filter(tuple => tuple._1 == "iri" || tuple._1 == "label"))
  }

}

case class MyNode(id: Long, properties: Map[String, Any])
case class MyEdge(id: Long, start: Long, end: Long, properties: Map[String, Any])
case class MyRow(source: MyNode, target: MyNode, edge: MyEdge)