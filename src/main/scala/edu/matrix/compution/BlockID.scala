package edu.matrix.compution

/**
 * @author Administrator
 */

/**
 * BlockID is the id of blocks splited from the matrix
 * 
 * @param blockRow rows number of the block in the matrix
 * @param blockCol column number of the block in the matrix
 * @param blockSeq sequence number of the block, default value is 0 
 */
class BlockID(val blockRow: Int, val blockCol: Int, val blockSeq: Int = 0) extends Serializable{
  
  override def equals(other: Any): Boolean = 
    other match{
      case that: BlockID => {
        blockRow == that.blockRow && blockCol == that.blockCol && blockSeq == that.blockSeq
      }
      case _ => false
  }
  
  override def hashCode(): Int = {
    blockRow * 31 + blockCol + blockSeq
  }
  
}