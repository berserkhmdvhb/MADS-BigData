
def topDocsForTermQuery(
                         US: RowMatrix,
                         V: Matrix,
                         query: BSparseVector[Double]): Seq[(Double, Long)] = {
  val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
  val termRowArr = (breezeV.t * query).toArray
  val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
  // first approach
  val docScores = UUS.columnSimilarities().toRowMatrix().multiply(termRowVec)
  // second approach
  //val USvec = US.rows
  //val US_normalised = USvec.map{ v => Vectors.dense(v.toArray.map(_/Vectors.norm(v, 2)))}
  //val docScores = US_normalised.multiply(termRowVec)
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

val US = multiplyByDiagonalRo0wMatrix(svd.U, svd.s)

val terms = List(" justice", " crime")
val queryVec = termsToQueryVector(terms, idTerms, idfs)
topDocsForTermQuery(US, svd.V, queryVec)


