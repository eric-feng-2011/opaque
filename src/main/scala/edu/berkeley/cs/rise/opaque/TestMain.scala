// Shell imports
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.GenericRunnerSettings
class TestMain {
  def exec(): Unit = {
    val settings = new GenericRunnerSettings( println _ )
        settings.usejavacp.value = true

    val conf = new SparkConf().setAppName("TestMain").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val methodChain =
      """
        val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
        val df = spark.createDataFrame(data).toDF("word", "count")
        df.show
      """
    
    val result = SparkILoop.run(methodChain, settings)
    println(result)

//    interpreter.bind("sqlContext" ,"org.apache.spark.sql.SQLContext", sqlContext)
//    val resultFlag = interpreter.interpret(methodChain)
  }

  def exec2(): Unit = {
    val settings = new GenericRunnerSettings( println _ )
        settings.usejavacp.value = true
    val methodChain =
      """
        df.show
      """
    val result = SparkILoop.run(methodChain, settings)
    println(result)    
  }

  def execProcess(args: Array[String]): Unit = {
    
    val builder = new ProcessBuilder("/home/opaque/spark-3.0.2-bin-hadoop2.7/bin/spark-shell");
    builder.redirectErrorStream(true);
    val process = builder.start();

    val stdin = process.getOutputStream ();
    val stdout = process.getInputStream ();

    // Buffer readers and writers
    val reader = new BufferedReader (new InputStreamReader(stdout));
    val writer = new BufferedWriter(new OutputStreamWriter(stdin));

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

    reader.close()
    writer.close()
  }
}

object TestInterpreter{

    def main(args: Array[String]): Unit = {
      val testMain = new TestMain()
      testMain.execProcess(args)
      System.exit(0)
    }}
