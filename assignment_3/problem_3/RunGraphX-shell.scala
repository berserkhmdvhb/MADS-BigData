// ------------------------ Disable excessive logging -------------------------

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// ----------- Parse the Medline Abstracts in XML Format ----------------------

import com.cloudera.datascience.common.XmlInputFormat

import scala.xml._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.conf.Configuration

def loadMedline(sc: SparkContext, path: String) = {
  @transient val conf = new Configuration()
  conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
  conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
  val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
    classOf[LongWritable], classOf[Text], conf)
  in.map(line => line._2.toString)
}

val medline_raw = loadMedline(sc, "../Data/medline-sample").sample(false, 0.01)
val medline_xml: RDD[Elem] = medline_raw.map(XML.loadString)

def majorTopics(elem: Elem): Seq[String] = {
  val dn = elem \\ "DescriptorName"
  val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
  mt.map(n => n.text)
}

val mesh_topics: RDD[Seq[String]] = medline_xml.map(majorTopics).cache()
mesh_topics.first
mesh_topics.count()

val topics: RDD[String] = mesh_topics.flatMap(mesh => mesh)
val topicCounts = topics.countByValue()
topicCounts.size

val tcSeq = topicCounts.toSeq
tcSeq.sortBy(_._2).reverse.take(20).foreach(println) // 20 most frequent MeSH topics
tcSeq.sortBy(_._2).take(100).foreach(println) // 100 least frequent MeSH topics

// analyze the distribution of topic frequencies (histogram)
val valueDist = topicCounts.groupBy(_._2).mapValues(_.size) 
valueDist.toSeq.sorted.take(20).foreach(println)

// create all distinct pairs of topics for each Medline abstract
val topicPairs = mesh_topics.flatMap(t => t.sorted.combinations(2))
val cooccurs = topicPairs.map(p => (p, 1)).reduceByKey(_ + _)
cooccurs.cache()
cooccurs.count()

val topicIds = topics.zipWithUniqueId().collectAsMap()
val bTopicIds = sc.broadcast(topicIds).value

def getTopicId(str: String) = {
  bTopicIds(str)
}

// ------- Create the Graph from the Co-Occurring MeSH Topics -----------------

import org.apache.spark.graphx._

val vertices = topics.map(topic => (getTopicId(topic), topic))
val edges = cooccurs.map(p => {
  val (topics, cnt) = p
  val ids = topics.map(getTopicId).sorted
  Edge(ids(0), ids(1), cnt)
})
val topicGraph = Graph(vertices, edges)
topicGraph.cache()

vertices.count()
topicGraph.vertices.count()
topicGraph.edges.count()

// ---- Analyze the Distribution and Contents of the Connected Components -----

val connectedComponentGraph: Graph[VertexId, Int] =
  topicGraph.connectedComponents()

def sortedConnectedComponents(
  connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
  val componentCounts =
    connectedComponents.vertices.map(_._2).countByValue
  componentCounts.toSeq.sortBy(_._2).reverse
}

val nameCID = topicGraph.vertices.
  innerJoin(connectedComponentGraph.vertices) {
    (topicId, name, componentId) => (name, componentId)
  }

val componentCounts = sortedConnectedComponents(
  connectedComponentGraph)
componentCounts.size

val topComponentCounts = componentCounts.take(20)
topicIds.find(_._2 == topComponentCounts(0)._1) // show the largest connected component representative topic
topicIds.find(_._2 == topComponentCounts(1)._1) // show the second-largest connected component representative topic

// -------- Analyze the Degree Distribution (following the slides) ------------

val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
degrees.map(_._2).stats()

val singletonTopicGroups = mesh_topics.filter(x => x.size == 1)
singletonTopicGroups.count()

val singletonTopics = singletonTopicGroups.flatMap(mesh =>   
mesh).distinct()
singletonTopics.count()

val flatTopics = topicPairs.flatMap(p => p)
singletonTopics.subtract(flatTopics).count()

def topNamesAndDegrees(degrees: VertexRDD[Int],
    topicGraph: Graph[String, Int]): Array[(String, Int)] = {
  val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
    (topicId, degree, name) => (name, degree) }
  val ord = Ordering.by[(String, Int), Int](_._2)
  namesAndDegrees.map(_._2).top(10)(ord) 
}

topNamesAndDegrees(degrees, topicGraph).foreach(println)

// ----------- Apply Filtering According to the Chi-Square Statistic ----------

val N = mesh_topics.count()
val topicCountsRdd = topics.map(x => (getTopicId(x), 1)).reduceByKey(_ + _)
val topicCountGraph = Graph(topicCountsRdd, topicGraph.edges)

def chiSq(YY: Int, YB: Int, YA: Int, N: Long): Double = {
  val NB = N - YB
  val NA = N - YA
  val YN = YA - YY
  val NY = YB - YY
  val NN = N - NY - YN - YY
  val inner = (YY * NN - YN * NY) - N / 2.0
  N * math.pow(inner, 2) / (YA * NA * YB * NB)
}

val chiSquaredGraph = topicCountGraph.mapTriplets(triplet => {
  chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, N)
})
chiSquaredGraph.edges.map(x => x.attr).stats()

val interesting = chiSquaredGraph.subgraph(
  triplet => triplet.attr > 6.6)

// and compare the filtered graph with the original topic graph
interesting.edges.count
topicGraph.edges.count

// -------------- Clustering Coefficient Computation --------------------------

val triangleCountGraph = topicGraph.triangleCount()
  triangleCountGraph.vertices.map(x => x._2).stats()

val maxTriangleGraph = topicGraph.degrees.mapValues(d => d * (d - 1) / 2.0)
val clusterCoefficientGraph = triangleCountGraph.vertices.
    innerJoin(maxTriangleGraph) { 
      (vertexId, triCount, maxTris) => {
        if (maxTris == 0) 0 else triCount / maxTris } 
      }
clusterCoefficientGraph.map(_._2).sum() / topicGraph.vertices.count()

// --- Invoke a Pregel-style Iterative All-Pairs Shortest-Paths Computation ---

def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
  def minThatExists(k: VertexId): Int = {
    math.min(
      m1.getOrElse(k, Int.MaxValue),
      m2.getOrElse(k, Int.MaxValue))
  }
  (m1.keySet ++ m2.keySet).map {
    k => (k, minThatExists(k))
  }.toMap
}

def update(id: VertexId, state: Map[VertexId, Int],
  msg: Map[VertexId, Int]) = {
  mergeMaps(state, msg)
}

def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int],
  bid: VertexId) = {
  val aplus = a.map { case (v, d) => v -> (d + 1) }
  if (b != mergeMaps(aplus, b)) {
    Iterator((bid, aplus))
  } else {
    Iterator.empty
  }
}

def iterate(e: EdgeTriplet[Map[VertexId, Int], _]) = {
  checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
    checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
}

val sampleVertices = topicGraph.vertices.map(v => v._1).
  sample(false, 0.01).collect().toSet

val mapGraph = topicGraph.mapVertices((id, _) => {
  if (sampleVertices.contains(id)) {
    Map(id -> 0)
  } else {
    Map[VertexId, Int]()
  }
})

val start = Map[VertexId, Int]()
val res = mapGraph.pregel(start)(update, iterate, mergeMaps)

val paths = res.vertices.flatMap {
  case (id, m) =>
    m.map {
      // merge symmetric (s,t) and (t,s) pairs into same canonical pair
      case (k, v) =>
        if (id < k) { (id, k, v) } else { (k, id, v) }
    }
}.distinct()
paths.cache()
paths.map(_._3).filter(_ > 0).stats()

val hist = paths.map(_._3).countByValue()
hist.toSeq.sorted.foreach(println)

// these are the (s,s) pairs with a distance of 0
val zero_paths = paths.filter { case (s, t, v) => v == 0 }.top(20)

// ------- PageRank Computation Using the Built-in GraphX API -----------------

val ranks = topicGraph.pageRank(0.001, 0.15).vertices
val namesAndRanks = ranks.innerJoin(topicGraph.vertices) {
  (topicId, rank, name) => (name, rank)
}
val ord = Ordering.by[(String, Double), Double](_._2)
namesAndRanks.map(_._2).top(30)(ord).foreach(println)

// Finally Compare to a Simple Degree-Distribution

//val degrees: VertexRDD[Int] = topicGraph.degrees.cache() // using both in- and out-degrees (alternative)
val degrees: VertexRDD[Int] = topicGraph.inDegrees.cache() // only in-degrees (default)
val namesAndRanks = degrees.innerJoin(topicGraph.vertices) {
  (topicId, rank, name) => (name, rank)
}
val ord = Ordering.by[(String, Int), Int](_._2)
namesAndRanks.map(_._2).top(30)(ord).foreach(println)
