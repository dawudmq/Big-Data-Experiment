import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Wordcount{

  def main(args: Array[String]) {
    /*计时器*/
    val startTime: Long = System.currentTimeMillis
    
    /*读取数据*/
    val data = sc.textFile("file:///home/hadoop/Downloads/data/savedrecs.txt")/*切记不要写"file:///Home/Downloads/data/savedrecs.txt"*/

    /*统计词频*/
    val WordCount = data.
    flatMap(str=>str.split(" ")).
    filter(!_.isEmpty).
    map(word=>(word,1)).
    reduceByKey(_+_)

    /*给数据频词排序并输出*/
    val topWordCount = WordCount.map{case (word, count) => (count, word)}.sortByKey(false)
    println(topWordCount.take(5).foreach(x=>println(x)))


    /*存储数据*/
    WordCount.saveAsTextFile("hdfs://localhost:9000/user/hadoop/wordcount5")
    

    /*运行时间测试*/
    val endTime: Long = System.currentTimeMillis
    System.out.println("程序运行时间： " + (endTime - startTime) + "ms")


  }

}

