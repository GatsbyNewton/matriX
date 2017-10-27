package edu.matrix.compution

import breeze.linalg.DenseMatrix
import edu.matrix.utils.MatrixPartitions
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat

/**
 * @author Administrator
 */
class BlockMatrix(
    private[edu] val block: RDD[(BlockID, DenseMatrix[Double])],
    private var allRow: Long,          //rows number of the origin matrix
    private var allCol: Long,          //column number of the origin matrix
    private var blockRowNum: Int,      //block number of rows in origin matrix
    private var blockColNum: Int       //block number of column in origin matrix
    )extends Serializable{
  
  def this(block: RDD[(BlockID, DenseMatrix[Double])]) = this(block, 0L, 0L, 0, 0)
  
  def numRow(): Long ={
    if(allRow <= 0L){
      allRow = block.filter(_._1.blockCol == 0).map(_._2.rows).reduce(_ + _)
    }
    allRow
  }
  
  def numCol(): Long ={
    if(allCol <= 0L){
      allCol = block.filter(_._1.blockRow == 0).map(_._2.cols).reduce(_ + _)
    }
    allCol
  }
  
  def test(): Int = {
    blockRowNum = block.filter(_._1.blockCol == 0).count().toInt
    blockRowNum
  }
  
//  def numBlockCols(): Int ={
//    if(blockColNum <= 0L){
//      blockColNum = block.filter(_._1.blockRow == 0).count().toInt
//    }
//    blockColNum
//  }
//  
//  def numBlockRows(): Int ={
//    if(blockRowNum <= 0L){
//      blockRowNum = block.filter(_._1.blockCol == 0).count().toInt
//    }
//    blockRowNum
//  }
  
  def multipy(other: BlockMatrix, _mSplitNum: Int, _kSplitNum: Int, _nSplitNum: Int, cores: Int = 16): BlockMatrix ={
    //block number of rows in former origin matrix
    val mSplitNum = _mSplitNum
    println("mSplitNum===============================================>>>>>>>" + mSplitNum)
    //block number of column in former origin matrix, meanwhile block number of rows in later origin matrix
    val kSplitNum = _kSplitNum
    println("kSplitNum===============================================>>>>>>>" + kSplitNum)
    //block number of column in later origin matrix
    val nSplitNum = _nSplitNum
    println("nSplitNum===============================================>>>>>>>" + nSplitNum)
    val partitioner = new MatrixPartitions(mSplitNum, kSplitNum, nSplitNum)
    
    val formerBlock = block.mapPartitions(iter =>
      iter.flatMap(it =>{
    	    val copyBlock = Array.ofDim[(BlockID, DenseMatrix[Double])](nSplitNum)    
          for(i <- 0 until nSplitNum){
            val blockSeq = it._1.blockRow * nSplitNum * kSplitNum + i * kSplitNum + it._1.blockCol
            copyBlock(i) = (new BlockID(it._1.blockRow, i, blockSeq), it._2)
          }
          copyBlock
        }
    ))
    val laterBlock = other.block.mapPartitions(iter =>
      iter.flatMap(it =>{
        val copyBlock = Array.ofDim[(BlockID, DenseMatrix[Double])](mSplitNum)
        for(i <- 0 until mSplitNum){
          val blockSeq = i * nSplitNum * kSplitNum + it._1.blockCol * kSplitNum + it._1.blockRow
          copyBlock(i) = (new BlockID(i, it._1.blockCol, blockSeq), it._2)
        }
        copyBlock
      }
    ))
    
    if(kSplitNum != 1){
      val result = formerBlock.join(laterBlock)
        .mapPartitions(iter =>
          iter.map(it =>{
            val former = it._2._1.asInstanceOf[DenseMatrix[Double]]
            val later = it._2._2.asInstanceOf[DenseMatrix[Double]]
            val product = (former * later).asInstanceOf[DenseMatrix[Double]]
            (new BlockID(it._1.blockRow, it._1.blockCol), product)
          }
      )).cache()
//      result
      new BlockMatrix(result, this.numRow(), this.numCol(), mSplitNum, nSplitNum)
    }
    else{
      val result = formerBlock.join(laterBlock)
        .mapPartitions(iter =>
          iter.map(it =>{
            val former = it._2._1.asInstanceOf[DenseMatrix[Double]]
            val later = it._2._2.asInstanceOf[DenseMatrix[Double]]
            val product = (former * later).asInstanceOf[DenseMatrix[Double]]
            (new BlockID(it._1.blockRow, it._1.blockCol), product)
          }
       )).cache()
//       result
       new BlockMatrix(result, this.numRow(), this.numCol(), mSplitNum, nSplitNum)
    }
  }
  
  
  /**
   * this function is used to save the martrix in DenseVecMatrix format
   * @param path the path to store in HDFS
   */
  def saveToFileSystem(path: String) {
    saveToFileSystem(path, " ")
  }
  
  /**
   * Save the result to the HDFS
   *
   * @param path the path to store in HDFS
   */
  def saveToFileSystem(path: String, format: String = " "){
    println("========================================Write to HDFS=====================================================")
    block.map(t => (NullWritable.get(), new Text(t._1.blockRow + "-" + t._1.blockCol
      + "-" + t._2.rows + "-" + t._2.cols + ":" + t._2.data.mkString(","))))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }
  
  def combineResult(allColumns: Long): RDD[DenseMatrix[Double]] = {
    println("===================================COMBINE MATRIX==================================================")
    val result = block.mapPartitions(iter =>
      iter.map(it =>{
        (it._1.blockRow, (it._1.blockCol, it._2))    
      }
    ))
    .groupByKey().mapPartitions(iter =>
      iter.map(it =>{
        val blockMatrix = Array.ofDim[DenseMatrix[Double]](blockColNum)
//        val blockRows = this.numBlockRows()
        val allColumn = allColumns
        
        val blocks = it._2.iterator
        while(blocks.hasNext){
          val tmp = blocks.next()
          blockMatrix(tmp._1) = tmp._2
        }
        
        val blockRows = blockMatrix(0).rows
        val arrBuff = Array.ofDim[Double](blockRows * allColumn.toInt)
        val length = blockMatrix.length
        val exceptLaterBlkCols = blockMatrix(0).cols
        var j = 0
        for(i <- 0 until length){
          val buff = blockMatrix(i).t.toDenseVector.toArray
          val blockCols = blockMatrix(i).cols
          val rowsOffset = allColumn.toInt - blockCols
          val size = buff.length
          var slices = 0        //rows number of buff stored
          for(k <- 0 until size){
            arrBuff(j * exceptLaterBlkCols + k + slices * rowsOffset) = buff(k)
        		if((k + 1) % blockCols == 0){
        			slices += 1
        		}
          }
        	j += 1
        }
        val mat = new DenseMatrix(allColumn.toInt, blockRows, arrBuff)
        mat.t
      }
    ))
    result
  }
  
  
}







