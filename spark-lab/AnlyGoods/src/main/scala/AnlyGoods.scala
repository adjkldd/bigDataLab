import org.apache.spark._
import org.apache.spark.streaming._


object AnlyGoods extends App {
  if (args.length < 3) {
    println("usage: AnlyGoods <host> <port> <checkpoint directory>")
    System.exit(0)
  }
  val conf = new SparkConf().setAppName("AnlyGoods")
  val sc = new StreamingContext(conf, Seconds(1))
  sc.checkpoint(args(2))
  // export HADOOP_USER_NAME=hadoop when use hdfs directory
  // sc.checkpoint("hdfs://172.17.0.2:9000/user/hadoop/AnlyGoodsCheckpoint")
  // sc.checkpoint("/home/ldd/big-data/spark-lab/AnlyGoods/foo")

  val host = args(0)
  val port = args(1).toInt
  val goodsDataStream = sc.socketTextStream(host, port)

  def updateFunc(newValues: Seq[Double], prevStat: Option[Double]): Option[Double] = {
    if (prevStat.isEmpty) {
      Some(newValues.sum)
    } else {
      Some(newValues.sum + prevStat.get)
    }
  }

  val favorDegree = goodsDataStream.map(row => {
    val col = row.split("::")
    val id = col(0)
    val browseTimes = col(1).toDouble
    val stayTime = col(2).toDouble
    val isCollected = col(3).toDouble
    val num_bought = col(3).toDouble

    val favorDegree = browseTimes * 0.8 + stayTime * 0.6 + isCollected * 1 + num_bought * 1
    (id, favorDegree)
  }).updateStateByKey(updateFunc _)
                   

  favorDegree.foreachRDD(rdd => {
    val topTen = rdd.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
    println("GOODS  GOODS_FAVOR")
    for (x <- topTen)
      println(x._1.formatted("%-7s") + " " + x._2.formatted("%.3f"))
  })


  sc.start()
  sc.awaitTermination()

}
