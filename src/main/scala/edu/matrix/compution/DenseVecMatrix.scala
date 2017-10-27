package edu.matrix.compution

import breeze.linalg.DenseMatrix
import breeze.linalg.{DenseVector => DVector}
import org.apache.spark.Spark
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer

/**
 * @author Administrator
 */

class DenseVecMatrix (
    private[edu] val vectors: RDD[(Long, DenseVector)],
    private var rows: Long,        //rows number of origin matrix
    private var column: Long       //column number of origin matrix
    )extends Serializable{
  
  def this(r: RDD[(Long, DenseVector)]) = this(r, 0L, 0)
  
  def numCol(): Long ={
    if(column <= 0){
      column = vectors.first()._2.size
    }
    column
  }
  
  def numRow(): Long ={
    if(rows <= 0){
      rows = vectors.map(_._1).reduce(math.max) + 1L
    }
    rows
  }
  
  
  def toMatrix(blockRow: Int, blockCol: Int): BlockMatrix = {
      
      val allRows = this.numRow()          //rows number of origin matrix
      val allColumn = this.numCol()        //column number of origin matrix  
      println("Rows=======================>>>>>>>"+allRows)
      println("Block========================>>>>>"+blockRow)
//      val blockRowSize = math.ceil(allRows / blockRow).toInt          //rows number of sub matrix
      val blockRowSize = math.ceil(allRows / blockRow).toInt
      val blockColSize = math.ceil(allColumn / blockCol).toInt          //column number of sub matrix
      val blockRowNum = math.ceil(allRows / blockRowSize).toInt         //block number of rows in origin matrix
      val blockColNum = math.ceil(allColumn / blockColSize).toInt       //block number of column in origin matrix
      println("RowSize==========================>>>>"+blockRowSize)
      println("ColSize==========================>>>>"+blockColSize)
      println("RowNum==========================>>>>"+blockRowNum)
      println("ColSize==========================>>>>"+blockColSize)
      
      if(blockColNum == 1){
        val result = vectors.mapPartitions(it => {
            it.map{ t => 
              (t._1.toInt / blockRowSize, t)
            }
        })
        .groupByKey().mapPartitions(it => 
          it.map{t =>
            val blockRowSeq = t._1    //block sequence number of rows in sub matrix
            val rowSize = if((blockRowSeq + 1) * blockRowSize > rows){
              rows.toInt - blockRowSeq * blockRowSize
            }else{
              blockRowSize
            }
            val iter = t._2.iterator
            val subMatrix = DenseMatrix.zeros[Double](rowSize, column.toInt)
            while(iter.hasNext){
              val (rowsIndex, vector) = iter.next()
              vector.toArray.zipWithIndex.map(x =>
                  subMatrix.update(rowsIndex.toInt - blockRowSeq * blockRowSize, x._2, x._1))
            }
            (new BlockID(t._1, 0), subMatrix)
          })
//          result
          new BlockMatrix(result, allRows, allColumn, blockRowNum, blockColNum)
      }
      else{
        val result = vectors.mapPartitions(it =>
          it.flatMap{ t =>
            var startColumn = 0
            var endColumn = 0
            var arrBuff = new ArrayBuffer[(BlockID, (Long, Vector))]
            val elem = t._2.toArray
            
            var i = 0
            while(endColumn < allColumn - 1){
              startColumn = i * blockColSize
              endColumn = startColumn + blockColSize - 1
              
              if(endColumn >= allColumn){
                endColumn = allColumn.toInt - 1
              }
              
              val arr = new Array[Double](endColumn - startColumn + 1)
              for(j <- startColumn to endColumn){
                arr(j - startColumn) = elem(j)
              }
              arrBuff += ((new BlockID(t._1.toInt / blockRowSize, i), (t._1, Vectors.dense(arr))))
              i += 1
            }
            arrBuff
          })
          .groupByKey().mapPartitions(iter =>
            iter.map{ it =>
                val colBase = it._1.blockCol * blockColSize
                val rowBase = it._1.blockRow * blockRowSize
                
                var subRows = blockRowSize      //rows number of sub matrix
                if((rowBase + blockRowSize - 1) >= allRows){
                  subRows = allRows.toInt - rowBase
                }
                
                var subCol = blockColSize      //column number of sub matrix
                if((colBase + blockColSize) >= allColumn){
                  subCol = allColumn.toInt - colBase
                }
                
                val arrMatrix = Array.ofDim[Double](subRows * subCol)
                val iterator = it._2.iterator
                while(iterator.hasNext){
                  
                  val vec = iterator.next()
                  val tmp = vec._2.toArray
                  
                  /**
                   * Due to creating matrix that the data must be column major by DenseMatrix and 
                   * sequential data of Array, rowOffset is used to set interval for Array data
                   * e.g
                   * array_1 = Array(1, 2), array_2 = Array(3, 4)
                   * arrMatrix = Array(1, 3, 2, 4)
                   * DenseMatrix: 1  2
                   *              3  4
                   */
                  val rowOffset = vec._1.toInt - rowBase
                  for(i <- 0 until tmp.length){
                    arrMatrix(i * subRows + rowOffset) = tmp(i)
                  }
                }
                val subMatrix = new DenseMatrix(subRows, subCol, arrMatrix)
                (it._1, subMatrix)
            })
//            result
            new BlockMatrix(result, allRows, allColumn, blockRowNum, blockColNum)
      }
     
  }
}