package edu.rdd

import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import edu.compution.DenseVector
import edu.utils.RandomDataGenerator
import edu.utils.RandomDataGenerator
import org.apache.spark.TaskContext
/**
 * @author Administrator
 */
private[edu] object RandomRDD{
  
  def getPartitions[T](
      size: Long,
      numPartitions: Int,
      generator: RandomDataGenerator[T],
      seed: Long): Array[Partition] ={
    
    val partitions = new Array[RandomRDDPartition[T]](numPartitions)
    var i = 0
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while(i < numPartitions){
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new RandomRDDPartition(i, start, (end - start).toInt, generator, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }
  
  // The generator has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getDenseVecIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int,
      rowsLength: Long): Iterator[(Long, DenseVector)] = {
    
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
//    generator.setSeed(System.nanoTime())
    val index = (0 + partition.start until rowsLength.toInt).toIterator
    
    Iterator.fill(partition.size)((index.next(),
//      new DenseVector(Array.fill(vectorSize)(generator.nextValue()))))
      new DenseVector(Array.fill(vectorSize)(System.currentTimeMillis()))))
//      new DenseVector(Array.fill(vectorSize)((System.currentTimeMillis()* 10000) - 1000))))
  }
}

//@SuppressWarnings("unchecked")
class RandomVecRDD(
    @transient sc: SparkContext,
    allRows: Long,
    vecSize: Int,
    numPartitions: Int,
    @transient generator: RandomDataGenerator[Double],
    @transient seed: Long = System.nanoTime()) extends RDD[(Long, DenseVector)](sc, Nil) {
  
  override def compute(splitIn: Partition, context: TaskContext): Iterator[(Long, DenseVector)] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    RandomRDD.getDenseVecIterator(split, vecSize, allRows)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(allRows, numPartitions, generator, seed)
  }
}

private[edu] class RandomRDDPartition[T](
    override val index: Int,
    val start: Long,
    val size: Int,
    val generator: RandomDataGenerator[T],
    val seed: Long) extends Partition{
  
  require(size >= 0, "Non-negative partition size required.")
} 