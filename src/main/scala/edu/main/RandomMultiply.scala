package edu.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import edu.utils.MatrixUtils
import edu.compution.DenseVecMatrix
import edu.compution.BlockMatrix

/**
 * @author Administrator
 */
object RandomMultiply {
  
  def main(args: Array[String]){
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    val rows = args(0)
    val row_col = args(1)
    val cols = args(2)
    val out = args(3)
    
    val splitNum = MatrixUtils.splitMethod(rows.toLong, row_col.toLong, cols.toLong)
    val tempA = MatrixUtils.randomDenVecMatrix(sc, rows.toLong, row_col.toInt)
    val tempB = MatrixUtils.randomDenVecMatrix(sc, row_col.toLong, cols.toInt)
//    val re = matrixA.toMatrix(splitNum._1, splitNum._2).collect()
//      .foreach(x => println("MatrixA::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
    
    val startTime = System.nanoTime()
    val subMatrixA = tempA.toMatrix(splitNum._1, splitNum._2)
    val subMatrixB = tempB.toMatrix(splitNum._2, splitNum._3)
    
    val mSplitNum = subMatrixA.block.filter(_._1.blockCol == 0).count().toInt
    val kSplitNum = subMatrixA.block.filter(_._1.blockRow == 0).count().toInt
    val nSplitNum = subMatrixB.block.filter(_._1.blockRow == 0).count().toInt
    val result = subMatrixA.multipy(subMatrixB, mSplitNum, kSplitNum, nSplitNum)
    result.block.collect().foreach(x => println("Matrix::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
    
      /** 
       *  when RDD[(BlockID, DenseMatrix[Double])] is type of subMatrixA.multipy(subMatrixB) returned, 
       *  val finalResult = (new BlockMatrix(result)).combineResult(colB) doesn't work
       */
//    val finalResult = (new BlockMatrix(result)).combineResult(colB)
    val finalResult = result.combineResult(cols.toLong)
    finalResult.collect().foreach(println)
    val endTime = System.nanoTime()
    println("Cost Time================================================>>>>>>" + (endTime - startTime))
      
    sc.stop()
  }
}