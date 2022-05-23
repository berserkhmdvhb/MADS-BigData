//////////////////////////////////////////////////////////////////////////////////////////////////

// PART THAT WILL BE USED TO SUBMIT ///// 

////////////////////////////////////////////////////////////////////////////////////////////////////////

import scala.xml._
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx._
import java.nio.file.{Paths, Files}

///// new (a)
val edges_data = spark.sparkContext.textFile("/home/maxsinner5/Documents/MADS_21_22/sem2/Big_Data_Analytics/assignment3/twitter_combined.txt").map(x => (x.split(" ")(0).toLong,x.split(" ")(1).toLong))
val part1 = edges_data.map(_._1)
val part2 = edges_data.map(_._2)
val merged_parts = part1.union(part2)
val vertices_data = merged_parts.distinct()
val vertices: RDD[(VertexId, (String))] = vertices_data.map(x => (x,(x.toString))) 
val part3:RDD[(VertexId, VertexId, (String))]= edges_data.map(x => (x._1,x._2,"Follows"))
val edges = part3.map(x => Edge(x._1,x._2,x._3))
val graph_non_labeled = Graph(vertices,edges)

///// b) Connected Components //////////////
graph_non_labeled.connectedComponents().vertices.map{case(_,cc) => cc}.distinct.count()
////The connected components is 1.

/////// (c) /////////

/////  Degree distribution /////////
val degrees: VertexRDD[Int] = graph_non_labeled.inDegrees.cache()
degrees.map(_._2).stats()

//// Clustering Coefficient
val triangleCountGraph = graph_non_labeled.triangleCount()
  triangleCountGraph.vertices.map(x => x._2).stats()

val maxTriangleGraph = graph_non_labeled.degrees.mapValues(d => d * (d - 1) / 2.0)
val clusterCoefficientGraph = triangleCountGraph.vertices.
    innerJoin(maxTriangleGraph) { 
      (vertexId, triCount, maxTris) => {
        if (maxTris == 0) 0 else triCount / maxTris } 
      }
clusterCoefficientGraph.map(_._2).sum() / graph_non_labeled.vertices.count()



/////// Paths ///////


//// Takes up to much memory to run ///////

//////

////// (d) ///////////

val ranks = graph_non_labeled.pageRank(0.001, 0.15).vertices
val namesAndRanks = ranks.innerJoin(graph_non_labeled.vertices) {
  (vertexId, rank, name) => (name, rank)
}
val ord = Ordering.by[(String, Double), Double](_._2)
namesAndRanks.map(_._2).top(250)(ord).foreach(println)

// Finally Compare to a Simple Degree-Distribution

val degrees: VertexRDD[Int] = graph_non_labeled.degrees.cache()
val namesAndRanks = degrees.innerJoin(graph_non_labeled.vertices) {
  (vertexId, rank, name) => (name, rank)
}
val ord = Ordering.by[(String, Int), Int](_._2)
namesAndRanks.map(_._2).top(250)(ord).foreach(println)

///////////////////////////////////////////////////////////////////////////////////////////////////////////

// PART THAT WILL BE USED TO SUBMIT ///// 

////////////////////////////////////////////////////////////////////////////////////////////////////////
