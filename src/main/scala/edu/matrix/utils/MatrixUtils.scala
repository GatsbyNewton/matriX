package edu.matrix.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.util.hashing.MurmurHash3
import java.nio.ByteBuffer

import edu.compution.Vectors
import edu.compution.DenseVecMatrix
import edu.matrix.compution.{DenseVecMatrix, Vectors}
import edu.matrix.rdd.RandomVecRDDs
import edu.rdd.RandomVecRDDs
import matrix.compution.{DenseVecMatrix, Vectors}

/**
 * @author Administrator
 */
object MatrixUtils{
  
  def loadMatrixFile(sc: SparkContext, path: String, minPartitions: Int = 4): DenseVecMatrix = {
    
    val file = sc.textFile(path, minPartitions)
    val rows = file.map(t =>{
      val e = t.split(":")
      val rowIndex = e(0).toLong
      val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
      val vec = Vectors.dense(array)
      (rowIndex,vec)
    }).cache()
//    rows.collect.foreach(x => println("Rows:: "+x._1+": "+x._2))
    
    new DenseVecMatrix(rows)
  }
  
  def randomDenVecMatrix(
      sc: SparkContext,
      allRows: Long,
      allColumns: Int,
      numPartitions: Int = 0,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): DenseVecMatrix ={
    
    val rows = RandomVecRDDs.randomDenVecRDD(sc, distribution, allRows, allColumns, numPartitionsOrDefault(sc, numPartitions)) 
    new DenseVecMatrix(rows, allRows, allColumns)
  }
  
  def splitMethod(rowsA: Long, row_col: Long, colB: Long, cores: Int = 16): (Int, Int, Int) = {
    
    var mSplitNum = 1
    var kSplitNum = 1
    var nSplitNum = 1
    var m = rowsA
    var k = row_col
    var n = colB
    var c = cores
    
    while(m > 1 && k > 1 && n > 1){
      if(n >= k && n >= m){
        nSplitNum *= 2
        n /= 2
        c /= 2
      }else if(m >= k && m >= n){
        mSplitNum *= 2
        m /= 2
        c /= 2
      }else if(k >= m && k >= n){
        kSplitNum *= 2
        k /= 2
        c /= 2
      }
    }
    (mSplitNum, kSplitNum, nSplitNum)
  }
  
  /** Hash seeds to have 0/1 bits throughout. */
  private[edu] def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
    MurmurHash3.bytesHash(bytes)
  }
  
  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions
    else if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    }else 
      sc.defaultMinPartitions
  }
}