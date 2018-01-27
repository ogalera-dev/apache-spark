import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]):Unit={
    if(args.length != 2){
      throw new RuntimeException("There must be two parameters!")
    }

    val conf = new SparkConf().setMaster("local").setAppName("Count words")
    val sc = new SparkContext(conf)

    //Load RDD
    val input = sc.textFile(args(0))
    val words = input.flatMap(x => x.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey{case(x, y) => x+y}
    result.saveAsTextFile(args(1))
  }
}
