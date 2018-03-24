import org.apache.spark.{SparkContext, SparkConf}
import scala.math.Ordering

case class cmbInfo(reportTime: Long, upPayload: Long, downPayload: Long)

object AnlyLog extends App {
  val conf = new SparkConf().setAppName(args(0)).setMaster(args(1))
  val sc = new SparkContext(conf)


  val lines = sc.textFile("/opt/resources/access_20170504.log")

  val p0 = lines.map(x => {
    val fs = x.split("\t")
    (fs(1), cmbInfo(fs(0).toLong, fs(2).toLong, fs(3).toLong))
  })


  val p1 = p0.reduceByKey((a, b) => {
    val t = a.reportTime < b.reportTime
    if (t) cmbInfo(a.reportTime, a.upPayload + b.upPayload, a.downPayload + b.downPayload)
    else cmbInfo(b.reportTime, a.upPayload + b.upPayload, a.downPayload + b.downPayload)
  })

  val p2 = p1.map(x => (x._2, x._1))
 
  implicit val multipleDimensionOrdering = new Ordering[cmbInfo] {
    override def compare(a: cmbInfo, b: cmbInfo) = {
      if (a.downPayload == b.downPayload) {
        if (a.upPayload == b.upPayload) {
          (a.reportTime - b.reportTime).toInt
        }
        else (a.upPayload - b.upPayload).toInt
      }
      else (a.downPayload - b.downPayload).toInt
    }
  }
  val p3 = p2.sortByKey(false)

  val p4 = p3.take(8)

  for(x <- p4) {
    val phoneNumber = x._2
    val info = x._1
    println(phoneNumber + " " + info.downPayload + " " + info.upPayload + " " + info.reportTime)
  }

  sc.stop()
}
