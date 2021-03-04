// Shell imports
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class TestMain {

  def execProcess(reader: BufferedReader, writer: BufferedWriter): Unit = {
    
    val query = 
      """
        val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
        println("hello world")
      """
    
    writer.write(query)
    writer.flush()

    var line: String = ""

    while ((line = reader.readLine ()) != null && ! line.trim().equals("hello world")) {
      System.out.println ("Stdout: " + line);
    }

    val query2 = 
      """
	val df = spark.createDataFrame(data).toDF("word", "count")
        df.show()
	println("hello world")
      """

    writer.write(query2)
    writer.flush()    

    while ((line = reader.readLine ()) != null && ! line.trim().equals("hello world")) {
      System.out.println ("Stdout: " + line);
    }

  }
 
  def execProcess2(reader: BufferedReader, writer: BufferedWriter): Unit = {
    
    val query = 
      """
        val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
        println("hello world")
      """
    
    writer.write(query)
    writer.flush()

    var line: String = ""

    while ((line = reader.readLine ()) != null && ! line.trim().equals("hello world")) {
      System.out.println ("Stdout: " + line);
    }

    val query2 = 
      """
	val df = spark.createDataFrame(data).toDF("word", "count")
        df.show()
	println("hello world")
      """

    writer.write(query2)
    writer.flush()    

    while ((line = reader.readLine ()) != null && ! line.trim().equals("hello world")) {
      System.out.println ("Stdout: " + line);
    }

  }
}

object TestInterpreter{

    def main(args: Array[String]): Unit = {
      val testMain = new TestMain()

      val builder = new ProcessBuilder("/home/opaque/spark-3.0.2-bin-hadoop2.7/bin/spark-shell");
      builder.redirectErrorStream(true);
      val process = builder.start();

      val stdin = process.getOutputStream ();
      val stdout = process.getInputStream ();

      val reader = new BufferedReader (new InputStreamReader(stdout));
      val writer = new BufferedWriter(new OutputStreamWriter(stdin));

      testMain.execProcess(reader, writer)
      testMain.execProcess2(reader, writer)

      reader.close()
      writer.close()

      System.exit(0)
    }}
