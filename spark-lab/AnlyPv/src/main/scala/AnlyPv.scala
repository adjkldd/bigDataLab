//
// $ mkfifo /tmp/f
// $ cat /tmp/f | nc -lk localhost 9999 > /tmp/f
// $ ./genLog.sh
//
// $ spark-submit --class AnlyPv --master spark://172.17.0.2:7077 target/scala-2.11/anlypv_2.11-1.0.jar 172.20.10.11 9999 | grep -v INFO
//
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 132.72.30.29 - - [2018-03-15 23:09:51] "GET /login.php HTTP/1.1" 200 0 "http://www.google.cn/search?q=spark" "Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19" "-"
// 168.187.63.201 - - [2018-03-15 23:09:51] "GET /index.html HTTP/1.1" 200 0 "-" "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)" "-"

object AnlyPv extends App {

  // val checkpointDir = "hdfs:///opt/streaming"

  def CreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("AnlyPv")
    val ssc = new StreamingContext(conf, Seconds(batch)) 
    // ssc.checkpoint(checkpointDir)
    ssc
  }

  val batch = 10

  // val ssc = StreamingContext.getOrCreate(checkpointDir, CreateContext _)
  val conf = new SparkConf().setAppName("AnlyPv")
  val ssc = new StreamingContext(conf, Seconds(batch))

  val lines = ssc.socketTextStream(args(0), args(1).toInt)

  // total pv
  lines.count().print()
  // lines.map((_, 1)).updateStateByKey((newValues: Seq[Int], runningCount: Option[Int]) => {
  //   val newCount = runningCount.get + newValues.sum
  //   Some(newCount)
  // }).print()
  

  // pv per ip
  lines.map(x => (x.split(' ')(0), 1)).reduceByKey(_+_).
        transform(rdd => {
          rdd.map(ip_pv => (ip_pv._2, ip_pv._1)).
              sortByKey(false).map(ip_pv => (ip_pv._2, ip_pv._1))
        }).print()


  // pv per search engine
  lines.map(_.split('\"')(3)).filter(_ != "-").
        map(x => (x.split('/')(2), 1)).reduceByKey(_+_).print()


  // pv per search keyword
  lines.map(_.split("\"")(3)).filter(_ != "-").
        map(url => {
          val f = url.split("/")
          // `searchEngines` must be inside the closure
          val searchEngines = Map(
            "www.google.com" -> "q",
            "www.google.cn" -> "q",
            "www.yahoo.com" -> "p",
            "cn.bing.com" -> "q",
            "www.baidu.com" -> "wd",
            "www.sogou.com" -> "query"
          )
          if (f.length > 2) {
            val host = f(2)
            if (searchEngines.contains(host)) {
              (url.split('?')(1).
                   split('&').
                   filter(_.contains(searchEngines(host)+"="))(0).split('=')(1),
               1)
            } else (host, 0)
          } else ("", 0)
        }).
        filter(_._2 > 0).reduceByKey(_+_).print()


  // pv per device
  lines.map(_.split("\"")(5)).map(agent => {
    val types = Seq("iPhone", "Android")
    var r = "Default"
    for (t <- types) {
        if (agent.indexOf(t) != -1)
            r = t
    }
    (r, 1)
  }).reduceByKey(_ + _).print()


  // pv per page
  lines.map(line => {(line.split("\"")(1).split(" ")(1), 1)}).reduceByKey(_ + _).print()


  ssc.start()
  ssc.awaitTermination()
}
