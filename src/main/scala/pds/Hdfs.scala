package pds

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.io.IOUtils

object Hdfs extends App {
  def write(uri: String, filePath: String, data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()



  }

  def load(file : String): String ={
    val conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://172.31.249.250:8020")
    val filesystem = FileSystem.get(conf);
    val fileName = file;
    val hdfsReadPath = new Path(fileName);
    val inputStream = filesystem.open(hdfsReadPath);
    System.out.println(inputStream)
    val out = IOUtils.toString(inputStream, "UTF-8")
    System.out.println(out)

    inputStream.close();
    filesystem.close()
    return out

  }
}
