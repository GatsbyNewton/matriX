package edu.rdd

import org.apache.spark.SparkContext
import edu.utils.RandomDataGenerator
import edu.utils.OnesGenerator
import org.apache.spark.rdd.RDD
import edu.compution.DenseVector

/**
 * @author Administrator
 */
object RandomVecRDDs {
  
  def randomDenVecRDD(
      sc: SparkContext,
      generator: RandomDataGenerator[Double],
      allRows: Long,
      allColumn: Int,
      numPartitions: Int = 0,
      seed: Long = System.nanoTime()): RDD[(Long, DenseVector)] ={
    
    new RandomVecRDD(sc, allRows, allColumn, numPartitionsOrDefault(sc, numPartitions), new OnesGenerator(), seed)
  }
  
  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(
      sc: SparkContext, 
      numPartitions: Int): Int = {
    
    if (numPartitions > 0) numPartitions else sc.defaultMinPartitions
  }
}