

//////////subtask (e)///////





// We can now use 𝑽 to inspect the latent concepts represented in our plots

// First, we may want to rank terms by their similarity to the top concepts
// ------------------- Query the Latent Semantic Index ------------------------

// as stated, each top terms and documents should be under 25 latent concepts
val numConcepts = 25
val numTerms = 25
val numDocs = 25


def topTermsInTopConcepts(
                           svd: SingularValueDecomposition[RowMatrix, Matrix],
                           numConcepts: Int, numTerms: Int): Seq[Seq[(String, Double)]] = {
  val v = svd.V
  val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
  val arr = v.toArray
  for (i <- 0 until numConcepts) {
    val offs = i * v.numRows
    val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
    val sorted = termWeights.sortBy(-_._1)
    topTerms += sorted.take(numTerms).map {
      case (score, id) =>
        (bIdTerms.find(_._2 == id).getOrElse(("", -1))._1, score)
    }
  }
  topTerms
}

def topDocsInTopConcepts(
                          svd: SingularValueDecomposition[RowMatrix, Matrix],
                          numConcepts: Int, numDocs: Int): Seq[Seq[(String, Double)]] = {
  val u = svd.U
  val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
  for (i <- 0 until numConcepts) {
    val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
    topDocs += docWeights.top(numDocs).map {
      case (score, id) => (docIds(id), score)
    }
  }
  topDocs
}


val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, numTerms)
val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, numDocs)
for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
  println("Concept terms: " + terms.map(_._1).mkString(", "))
  println("Concept docs: " + docs.map(_._1).mkString(", "))
  println()
}

// Keyword Queries

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}

def termsToQueryVector(
                        terms: scala.collection.immutable.Seq[String],
                        idTerms: scala.collection.immutable.Map[String, Int],
                        idfs: scala.collection.immutable.Map[String, Double]): BSparseVector[Double] = {
  val indices = terms.map(idTerms(_)).toArray
  val values = terms.map(idfs(_)).toArray
  new BSparseVector[Double](indices, values, idTerms.size)
}


// This needs to be edited
def topDocsForTermQuery(
                         US: RowMatrix,
                         V: Matrix,
                         query: BSparseVector[Double]): Seq[(Double, Long)] = {
  val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
  val termRowArr = (breezeV.t * query).toArray
  val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
  val docScores = US.multiply(termRowVec)
  val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()
  allDocWeights.top(10)
}

def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
  val sArr = diag.toArray
  new RowMatrix(mat.rows.map { vec =>
    val vecArr = vec.toArray
    val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
    Vectors.dense(newArr)
  })
}

val US = multiplyByDiagonalRowMatrix(svd.U, svd.s)

val terms = List(" justice", " crime")
val queryVec = termsToQueryVector(terms, idTerms, idfs)
topDocsForTermQuery(US, svd.V, queryVec)



// FOR reporting top frequent Genres
val features_rdd = features_df.select("Genre", "Features").rdd.map{row => (row.getString(0), row(1).toString.split(',').toSeq)}
features_rdd.cache()
val docTermFreqs = features_rdd.map {
  case (title, terms) => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
      (map, term) =>
      {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    (title, termFreqs)
  }
}
docTermFreqs.cache()
docTermFreqs.count()

val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
val docFreqs = docTermFreqs.map(_._2).flatMap(_.keySet).map((_, 1)).
  reduceByKey(_ + _, 24)
val numTerms = 5000
val ordering = Ordering.by[(String, Int), Int](_._2)
val topDocFreqs = docFreqs.top(numTerms)(ordering)

val idfs = topDocFreqs.map {
  case (term, count) =>
    (term, math.log(bNumDocs.value.toDouble / count))
}.toMap
val idTerms = idfs.keys.zipWithIndex.toMap
val termIds = idTerms.map(_.swap)
val bIdfs = sc.broadcast(idfs).value
val bIdTerms = sc.broadcast(idTerms).value
val vecs = docTermFreqs.map(_._2).map(termFreqs => {
  val docTotalTerms = termFreqs.values.sum
  val termScores = termFreqs.filter {
    case (term, freq) => bIdTerms.contains(term)
  }.map {
    case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
  }.toSeq
  Vectors.sparse(bIdTerms.size, termScores)
})
vecs.cache()
vecs.count()
val k = 25
val mat = new RowMatrix(vecs)
val svd = mat.computeSVD(k, computeU = true)
val numConcepts = 25
val numTerms = 25
val numDocs = 25
def topDocsInTopConcepts(
                          svd: SingularValueDecomposition[RowMatrix, Matrix],
                          numConcepts: Int, numDocs: Int): Seq[Seq[(String, Double)]] = {
  val u = svd.U
  val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
  for (i <- 0 until numConcepts) {
    val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
    topDocs += docWeights.top(numDocs).map {
      case (score, id) => (docIds(id), score)
      counter += 1
    }
  }
  topDocs
}



//val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, numTerms)
val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, numDocs)






// For reporting top frequent Genres, we select genre and features this time
val features_rdd = features_df.select("Genre", "Features").rdd.map{row => (row.getString(0), row(1).toString.split(',').toSeq)}
features_rdd.cache()
val docTermFreqs = features_rdd.map {
  case (title, terms) => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
      (map, term) =>
      {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    (title, termFreqs)
  }
}
docTermFreqs.cache()
docTermFreqs.count()

val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
val docFreqs = docTermFreqs.map(_._2).flatMap(_.keySet).map((_, 1)).
  reduceByKey(_ + _, 24)
val numTerms = 5000
val ordering = Ordering.by[(String, Int), Int](_._2)
val topDocFreqs = docFreqs.top(numTerms)(ordering)

val idfs = topDocFreqs.map {
  case (term, count) =>
    (term, math.log(bNumDocs.value.toDouble / count))
}.toMap
val idTerms = idfs.keys.zipWithIndex.toMap
val termIds = idTerms.map(_.swap)
val bIdfs = sc.broadcast(idfs).value
val bIdTerms = sc.broadcast(idTerms).value
val vecs = docTermFreqs.map(_._2).map(termFreqs => {
  val docTotalTerms = termFreqs.values.sum
  val termScores = termFreqs.filter {
    case (term, freq) => bIdTerms.contains(term)
  }.map {
    case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
  }.toSeq
  Vectors.sparse(bIdTerms.size, termScores)
})
vecs.cache()
vecs.count()
val k = 25
val mat = new RowMatrix(vecs)
val svd = mat.computeSVD(k, computeU = true)
val numConcepts = 25
val numTerms = 25
val numDocs = 25
def topDocsInTopConcepts(
                          svd: SingularValueDecomposition[RowMatrix, Matrix],
                          numConcepts: Int, numDocs: Int): Seq[Seq[(String, Double)]] = {
  val u = svd.U
  val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
  for (i <- 0 until numConcepts) {
    val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
    topDocs += docWeights.top(numDocs).map {
      case (score, id) => (docIds(id), score)
      counter += 1
    }
  }
  topDocs
}



//val topConceptTerms = topTermsInTopConcepts(svd, numConcepts, numTerms)
val topConceptDocs = topDocsInTopConcepts(svd, numConcepts, numDocs)




//calculate runtime
val stop = System.nanoTime()
val diff_in_s = (stop - start) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed time: ${mins} min ${secs} s")



