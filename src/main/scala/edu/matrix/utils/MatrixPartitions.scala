package edu.matrix.utils

import edu.matrix.compution.BlockID
import org.apache.spark.Partitioner

/**
 * @author Administrator
 */
class MatrixPartitions(
    val mSplitNum: Int,
    val kSplitNum: Int,
    val nSplitNum: Int) extends Partitioner{
  
  override def numPartitions: Int = {
    if(mSplitNum >= kSplitNum && mSplitNum >= nSplitNum){
      mSplitNum
    }
    else if(nSplitNum >= mSplitNum && nSplitNum >= kSplitNum){
      nSplitNum
    }
    else{
      kSplitNum
    }
  }
  
  override def getPartition(key: Any): Int ={
    key match{
      case (blockID: BlockID) => blockID.blockSeq
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }
  
  override def equals(obj: Any): Boolean = {
    obj match {
      case p: MatrixPartitions =>
        (this.mSplitNum == p.mSplitNum) && (this.kSplitNum == p.kSplitNum) &&
          (this.nSplitNum == p.nSplitNum)
      case _ =>
        false
    }
  } 
}