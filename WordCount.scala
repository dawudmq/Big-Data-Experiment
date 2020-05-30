import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io._

object Wordcount{

  /*配置HDFS文件系统*/
  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  /*获取路径*/
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /*返回目录下的路径*/
  def listFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }


  def main(args: Array[String]) {
    /*计时器*/
    val startTime: Long = System.currentTimeMillis
    
    /*数据路径*/
    val path = "hdfs://localhost:9000/user/hadoop/data"
    val file_array = listFiles(path)

    /*读取所有文件的数据*/
    val data = sc.textFile("hdfs://localhost:9000/user/hadoop/data")/*注意这里的路径是初始化用的，可以随便写一个可达的路径*/
    for (e <- file_array){
        val t= e
        data ++ sc.textFile(t.toString)
    }

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

