package edu.matrix.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import edu.utils.MatrixUtils
import edu.compution.DenseVecMatrix
import edu.compution.BlockMatrix
import edu.matrix.utils.MatrixUtils
import matrix.utils.MatrixUtils

/**
 * @author Administrator
 */
object MatrixMultiply {
  
  def main(args: Array[String]){
    
    if(args.length != 7){
      println("Usage: <matrixA> <matrixB> <output> <rowsA> <colsA> <colsB> <verbose>")
      System.exit(1)
    }
    val startJob = System.currentTimeMillis()
    
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    //the path of the matrix A
    val pathA = args(0)
    //the path of the matrix B
    val pathB = args(1)
    //the path of the multiply result
    val outPath = args(2)
    //the rows number of the matrix A
    val rowsA = args(3).toLong
    //the column number of the matrix A, meanwhile the rows number of the matrix B
    val row_col = args(4).toLong
    //the column number of the matrix B
    val colB = args(5).toLong
    
    val verbose = args(6)
    
    val splitNum = MatrixUtils.splitMethod(rowsA, row_col, colB)
    
//    val subMatrixA = MatrixUtils.loadMatrixFile(sc, pathA)
//    val subMatrixB = MatrixUtils.loadMatrixFile(sc, pathB)
//    subMatrixA.toMatrix(splitNum._1, splitNum._2).collect()
//      .foreach(x => println("MatrixA::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
//    subMatrixB.toMatrix(splitNum._2, splitNum._3).collect()
//      .foreach(x => println("MatrixB::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
    val tempA = MatrixUtils.loadMatrixFile(sc, pathA)
    val tempB = MatrixUtils.loadMatrixFile(sc, pathB)
    val startComputing = System.currentTimeMillis()
    val subMatrixA = tempA.toMatrix(splitNum._1, splitNum._2)
    val subMatrixB = tempB.toMatrix(splitNum._2, splitNum._3)
    
//    subMatrixB.block.filter(_._1.blockRow == 0).collect().foreach(x => println("MatrixB::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
//    val num = subMatrixB.block.filter(_._1.blockRow == 0).count().toInt
    
//    subMatrixA.test().collect().foreach(x => println("Block::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
//    println("subMatrixA Test======================================>>>>>>>"+subMatrixA.test())
//    println("subMatrixA Rows======================================>>>>>>>"+subMatrixA.numRow())
//    println("subMatrixA BlkRows======================================>>>>>>>"+subMatrixA.numBlockRows())
//    println("=========================="+num+"==================================")
//    println("subMatrixB Cols======================================>>>>>>>"+subMatrixB.numCol())
//    println("subMatrixB BlkCols======================================>>>>>>>"+subMatrixB.numBlockCols())
//    subMatrixB.block.collect().foreach(x => println("MatrixB::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
/////////////////////////////////////////////////////////////////////////////////////////////////////    
    val mSplitNum = subMatrixA.block.filter(_._1.blockCol == 0).count().toInt
    val kSplitNum = subMatrixA.block.filter(_._1.blockRow == 0).count().toInt
    val nSplitNum = subMatrixB.block.filter(_._1.blockRow == 0).count().toInt
    val result = subMatrixA.multipy(subMatrixB, mSplitNum, kSplitNum, nSplitNum)
//    result.block.collect().foreach(x => println("Matrix::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
    
      /** 
       *  when RDD[(BlockID, DenseMatrix[Double])] is type of subMatrixA.multipy(subMatrixB) returned, 
       *  val finalResult = (new BlockMatrix(result)).combineResult(colB) doesn't work
       */
//    val finalResult = (new BlockMatrix(result)).combineResult(colB)
    val endComputing = System.currentTimeMillis()
    
    if(verbose.equals("true")){
      result.block.collect().foreach(x => println("Matrix::( "+x._1.blockRow+","+x._1.blockCol+"): \n"+x._2))
    }
    
    result.saveToFileSystem(outPath)
    
//    val finalResult = result.combineResult(colB)
//    finalResult.saveAsTextFile(outPath)
//    finalResult.collect.foreach(println)
	
    sc.stop()
    val endJob = System.currentTimeMillis()
    println("Computing Cost Time==========================================>>>>>>" + (endComputing - startComputing))
    println("Job Cost Time================================================>>>>>>" + (endJob - startJob))
  }
}