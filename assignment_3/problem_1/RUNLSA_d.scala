


//////////subtask (d)///////

// compute term frequency for each title
//val features = features_df.select("Title", "Features").rdd.map{row => (row.getString(0), row.getString(1))}
//val temp = features_rdd.map(_.mkString(","))
val features_rdd = features_df.select("Title", "Features").rdd.map{row => (row.getString(0), row(1).toString.split(',').toSeq)}
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



//To further reduce the term space, we may run a standard word count to
//filter out infrequent terms (in this case with a document frequency of less
//than 24):
val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
val docFreqs = docTermFreqs.map(_._2).flatMap(_.keySet).map((_, 1)).
  reduceByKey(_ + _, 24)


//Keep only the top 5,000 terms from within all documents:
val numTerms = 5000
val ordering = Ordering.by[(String, Int), Int](_._2)
val topDocFreqs = docFreqs.top(numTerms)(ordering)


// invert the DF values into their IDF counterparts:
val idfs = topDocFreqs.map {
  case (term, count) =>
    (term, math.log(bNumDocs.value.toDouble / count))
}.toMap

val idTerms = idfs.keys.zipWithIndex.toMap
val termIds = idTerms.map(_.swap)

val bIdfs = sc.broadcast(idfs).value
val bIdTerms = sc.broadcast(idTerms).value

// -------------  Prepare the Input Matrix and Compute the SVD ----------------

// The last transformation combines the TF and IDF weights for all terms
// in a document into a sparse vector representation:
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

// number of latent concepts in the reduced matrix
val k = 25


// The RowMatrix class is used to represent entries of a document-term
// matrix in a sparse way. It is directly used as input for the SVD computation:
val mat = new RowMatrix(vecs)
val svd = mat.computeSVD(k, computeU = true)

