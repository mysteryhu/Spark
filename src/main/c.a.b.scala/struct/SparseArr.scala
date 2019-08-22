package struct

import scala.collection.mutable.ArrayBuffer

object SparseArr {
  def main(args: Array[String]): Unit = {

    //演示一个稀疏数组的使用
    val chessMap: Array[Array[Int]] = Array.ofDim[Int](11,11)
    //初始化地图
    chessMap(1)(2) = 1 //1表示黑子
    chessMap(2)(3) = 2 //2 表示白子

    //输出原始地图
    for (elem <- chessMap) {
      for (elem1 <- elem) {

        //printf("%d\t",elem1)
      }
      //println()
    }

    //将chessMap转成稀疏数组
    //思路:达到对数据的压缩
    //class Node (row,col,value)
    //ArrayBuffer

    val sparseArr: ArrayBuffer[Node] = new ArrayBuffer[Node]()

    for(i<- 0 until chessMap.length ){

      for (j<- 0 until chessMap(i).length){
        //判断每个点上是否有值
        if(chessMap(i)(j)!=0){
          val node = new Node(i,j,chessMap(i)(j))
          sparseArr.append(node)
        }
      }
    }

    //打印稀疏数组
    for (node <- sparseArr) {
      printf("%d\t%d\t%d\t\n",node.row,node.col,node.value)
    }

    //存盘

    //读盘


  }
}

class Node(val row:Int,val col:Int,val value:Int)
