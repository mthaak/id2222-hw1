import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SimilarItems {
  def main(args: Array[String]) {
    // Set up Spark
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load document corpusses
    val docs = sc.wholeTextFiles("data/lay_k_inbox/", 2).cache()
      .map({ case (path, content) => (path.split("/").last, content) }) // keep only filename
      .mapValues(_.replace("\r\n", " ")) // replace newline characters with spaces

    // Break documents in shingles and hash them to 2^32 integers
    val k = 10
    val shingles = docs
      .mapValues(createShingles(_, k))
      .mapValues(hashShingles)
    val shinglesMap = shingles.collectAsMap()

    // Prepare hash functions for MinHashing
    val s = 120 // size signature
    val random = new Random()
    val hashFunctions = (1 to s)
      .map(_ => (random.nextInt(), random.nextInt())) // generate random a and b values
      .map({ case (a, b) => (x: Int) => (a * x + b) % Int.MaxValue }) // create hash functions
      .toList

    val t = 0.9 // min threshold

    // Calculate document signatures from their shingles using MinHashing
    val signatures = shingles
      .mapValues(calcMinHashSignature(_, hashFunctions))

    // Determine candidate pairs using LSH
    val candidatePairs = LSH(signatures, s, t)

    // Compare the signatures for each pair
    val candidatePairsWithSignatureComparison = candidatePairs
      .map({ case (doc1, doc2) => (doc1, doc2, compareSignatures(doc1._2, doc2._2)) })

    // Filter the pairs by comparing their signatures
    val candidatePairsFilteredBySignature = candidatePairsWithSignatureComparison
      .filter({ case (doc1, doc2, signComp) => signComp >= t })

    // Compare the shingles for each pair
    val candidatePairsWithShingleComparison = candidatePairsFilteredBySignature
      .map({ case (doc1, doc2, shinglesComp) =>
        val (shinglesDoc1, shinglesDoc2) = (shinglesMap.getOrElse(doc1._1, Set()), shinglesMap.getOrElse(doc2._1, Set()))
        (doc1, doc2, compareSets(shinglesDoc1, shinglesDoc2))
      })

    // Filter the pairs by comparing their shingles
    val finalPairs = candidatePairsWithShingleComparison
      .filter({ case (doc1, doc2, shinglesComp) => shinglesComp >= t })

    // Print results
    val printIntermediary = false
    if (printIntermediary) {
      println("== LSH candidate pairs with signature comparison ==")
      candidatePairsWithSignatureComparison.collect()
        .sortBy({ case (doc1, doc2, signComp) => -signComp })
        .foreach({ case (doc1, doc2, signComp) => println("%s x %s (%.6f)".format(doc1._1, doc2._1, signComp)) })

      println("\r\n== Filtered candidate pairs (t = %.6f) with shingles comparison ==".format(t))
      candidatePairsWithShingleComparison.collect()
        .sortBy({ case (doc1, doc2, shinglesComp) => -shinglesComp })
        .foreach({ case (doc1, doc2, shinglesComp) => println("%s x %s (%.6f)".format(doc1._1, doc2._1, shinglesComp)) })
    }

    println("\r\n== Final filtered pairs (t = %.6f) ==".format(t))
    finalPairs.collect()
      .sortBy({ case (doc1, doc2, shinglesComp) => -shinglesComp })
      .foreach({ case (doc1, doc2, shinglesComp) => println("%s x %s (%.6f)".format(doc1._1, doc2._1, shinglesComp)) })
  }


  //  1. A class Shingling that constructs k–shingles of a given length k (e.g., 10) from a given document,
  //  computes a hash value for each unique shingle, and represents the document in the form of an ordered set of its hashed k-shingles.
  def createShingles(doc: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(doc.substring(_).grouped(k))
      .filter(_.length == k)
      .toSet
  }

  // Hashes a set of shingles to a single integer
  def hashShingles(shingles: Set[String]): Set[Int] = {
    shingles.map(_.hashCode)
  }

  // 2. A class CompareSets that estimates the Jaccard similarity of two sets of integers – two sets of hashed shingles.
  def compareSets(A: Set[Int], B: Set[Int]): Double = {
    A.intersect(B).size.toDouble / A.union(B).size
  }

  // 3. A class MinHashing that builds a minHash signature (in the form of a vector or a set) of a given length n from a
  // given set of integers (a set of hashed shingles).
  def calcMinHashSignature(doc: Set[Int], functions: List[Int => Int]): List[Int] = {
    functions.map(h => doc.minBy(h(_)))
  }

  // 4. A class CompareSignatures that estimates similarity of two integer vectors – minhash signatures – as a fraction
  // of components, in which they agree.
  def compareSignatures(A: List[Int], B: List[Int]): Double = {
    A.zip(B).count({ case (a, b) => a == b }).toDouble / A.size.toDouble
  }

  // 5. A class LSH that implements the LSH technique: given a collection of minhash signatures (integer vectors) and
  // a similarity threshold t, the LSH class (using banding and hashing) finds all candidate pairs of signatures that
  // agree on at least fraction t of their components.
  def LSH(docs: RDD[(String, List[Int])], s: Int, t: Double): RDD[((String, List[Int]), (String, List[Int]))] = {
    val (b, r) = tuneLSRParams(s, t)
    val numBuckets = 1000000
    println("LSH: b = %d, r = %d, s = %d, numBuckets = %d, t ~ %.6f"
      .format(b, r, s, numBuckets, Math.pow(1 / b.toDouble, 1 / r.toDouble)))

    // Create a RDD (bucket, doc) where each doc is associated with multiple buckets
    val docsInBuckets = docs
      .flatMap({ case (doc, signature) =>
        signature.grouped(r).map(g => { // take r rows at a time
          val bucket = Math.floorMod(g.hashCode(), numBuckets) // hash to bucket
          (bucket, (doc, signature))
        })
      })

    // Determine candidate pairs
    val candidatePairs = docsInBuckets
      .groupByKey() // group by bucket
      .flatMap({ case (bucket, docsInBucket) => docsInBucket.crossProduct(docsInBucket) }) // take product per bucket
      .map({ case (doc1, doc2) => if (doc1._1 < doc2._1) (doc1, doc2) else (doc2, doc1) }) // since (x, y) = (y, x)
      .filter({ case (doc1, doc2) => doc1._1 != doc2._1 }) // remove pairs of the same document
      .distinct()

    candidatePairs
  }

  // Finds b, r such that b * r = s and |t - (1/b)^(1/r)| is minimal
  def tuneLSRParams(s: Int, t: Double): (Int, Int) = {
    (1 to s)
      .map(b => (b, s / b))
      .filter({ case (b, r) => b * r == s })
      .minBy({ case (b, r) => Math.abs(t - Math.pow(1 / b.toDouble, 1 / r.toDouble)) })
  }

  // Implicit class used for cross product of two lists
  private implicit class Crossable[X](xs: Traversable[X]) {
    def crossProduct[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for {x <- xs; y <- ys} yield (x, y)
  }

}