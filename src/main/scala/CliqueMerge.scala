import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._

object Hi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val nodes: RDD[(VertexId, (String))] =
      sc.parallelize(Array((1L, ("disease1")), (2L, ("disease2")), (5L, ("disease3")),
        (3L, ("phenotype1")), (4L, ("phenotype2"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, "sameAs"), Edge(3L, 4L, "sameAs"), Edge(2L, 5L, "sameAs"),
        Edge(2L, 4L, "hasPhenotype")))

    // Build the initial Graph
    val graph = Graph(nodes, relationships)

    val equivalentFirstPass = graph.aggregateMessages[Set[VertexId]](
      triplet => { // Map Function
        if (triplet.attr.equals("sameAs")) {
          triplet.sendToDst(Set(triplet.srcId))
          triplet.sendToSrc(Set(triplet.dstId))
          triplet.sendToDst(Set(triplet.dstId))
          triplet.sendToSrc(Set(triplet.srcId))
        }
      },
      (a, b) => (a ++: b) // Reduce Function
      )

    equivalentFirstPass.collect.foreach(f => {
      println(s"equivalent for ${f._1}")
      println(s"${f._2}")
    })

    val onlySets = equivalentFirstPass.map(f => {
      Seq(f._2)
    }).collect

    val cliques = onlySets.fold(Seq(Set.empty[VertexId]))(
      (acc, n) => {
        val mergedEntry: Set[VertexId] = acc.map { a =>
          {
            if (!a.intersect(n.head).isEmpty) {
              a.union(n.head)
            } else {
              Set.empty[VertexId]
            }
          }
        }.fold(Set.empty[VertexId])((acc, n) => {
          acc.++:(n)
        })

        if (mergedEntry.isEmpty) {
          acc.+:(n.head)
        } else {
          (acc.filter { a => a.intersect(n.head).isEmpty }).+:(mergedEntry)
        }
      }).distinct

    println(s"Cliques")
    cliques.foreach(f => {
      println(s"${f}")
    })

  }
}
