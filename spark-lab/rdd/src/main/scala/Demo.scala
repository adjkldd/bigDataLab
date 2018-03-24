import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Demo {
  def main(args: Array[String]) = {
    // println(args(0) + " " + args(1))
    val conf = new SparkConf().setAppName(args(0)).setMaster(args(1))
    val sc = new SparkContext(conf)
    
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.reduce((a, b) => a + b)

    sc.stop()
  }

}
