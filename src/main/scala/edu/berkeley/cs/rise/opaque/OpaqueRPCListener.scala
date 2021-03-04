package edu.berkeley.cs.rise.opaque

import java.util.logging.Logger

import com.google.protobuf.ByteString

import io.grpc.{Server, ServerBuilder}
import java.io.{File, BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.file.{Files, Path, Paths}
import scala.reflect.io.Directory

import rpc.Rpc
import rpc.OpaqueRPCGrpc

import scala.concurrent.{ExecutionContext, Future}
import org.apache.spark.sql.SparkSession

import implicits._

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldServer.java]]
 */
object OpaqueRPCListener {
  private val logger = Logger.getLogger(classOf[OpaqueRPCListener].getName)

  def main(args: Array[String]): Unit = {
    val server = new OpaqueRPCListener(ExecutionContext.global)
    server.start(args)
    server.blockUntilShutdown()
    println("Print hello world server")
  }

  private val port = 50051
}

class OpaqueRPCListener(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null
  private[this] var spark: SparkSession = null
  private[this] var reader: BufferedReader = null
  private[this] var writer: BufferedWriter = null
  private[this] var tempDir: Path = null

  // TODO: Currently hard-coded, but can make server generate random string for each query
  private[this] var magicString: String = "hello world"
  private[this] var magicQuery: String = "println(\"" + magicString + "\")" + "\n"
 
  private def start(args: Array[String]): Unit = {

    // initialize process and read until empty

    val spark_home = sys.env("SPARK_HOME")
    val spark_shell_bin = spark_home + "/" + "/bin/spark-shell"
    
    // First argument should be jar file. Second argument should be spark cluster. Otherwise no arguments
    var opaque_jar: String = null
    var spark_master: String = null
    var builder: ProcessBuilder = null
    if (args.length == 2) {
      opaque_jar = args(0)
      spark_master = args(1)
      builder = new ProcessBuilder(spark_shell_bin, "--jars", opaque_jar, "--master", spark_master, 
      "--conf", "spark.executor.instances=2");
    } else {
      builder = new ProcessBuilder(spark_shell_bin);
    }
    builder.redirectErrorStream(true);
    val process = builder.start();
    val stdin = process.getOutputStream ();
    val stdout = process.getInputStream ();
    reader = new BufferedReader (new InputStreamReader(stdout));
    writer = new BufferedWriter(new OutputStreamWriter(stdin));
    
    // Import Opaque libraries and flush spark initialization
    // Also create temp dir in opaque home to store enclave report and client keys

    val opaque_home = sys.env("OPAQUE_HOME")
    tempDir = Files.createDirectory(Paths.get(opaque_home, "tmp"))
    val subReportDir = Files.createDirectory(tempDir.resolve("report"))
    val subKeyDir = Files.createDirectory(tempDir.resolve("keys"))

    var methodChain = 
      """
        import edu.berkeley.cs.rise.opaque.implicits._

        edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
      """
    methodChain = methodChain + magicQuery
    writer.write(methodChain)
    writer.flush()
    var line: String = ""
    while ((line = reader.readLine ()) != null && ! line.trim().equals(magicString)) {
      println(line)
    }

    // Create rpc Server and make it listen
    server = ServerBuilder.forPort(OpaqueRPCListener.port).addService(new OpaqueRPCImpl()).build.start
    OpaqueRPCListener.logger.info("Server started, listening on " + OpaqueRPCListener.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      new Directory(tempDir.toFile()).deleteRecursively()
      reader.close()
      writer.close()
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class OpaqueRPCImpl extends OpaqueRPCGrpc.OpaqueRPCImplBase {

    override def relayGenerateReport(req: Rpc.RARequest, responseObserver: io.grpc.stub.StreamObserver[Rpc.RAReply]) = {
      
      val report = tempDir.resolve("report")
      val reportDirStream = Files.newDirectoryStream(report).iterator()

      val replyBuilder = Rpc.RAReply.newBuilder().setSuccess(true)
      while (reportDirStream.hasNext()) {
        val file = reportDirStream.next()
        val byteString: ByteString = ByteString.copyFrom(Files.readAllBytes(file))
        replyBuilder.addReport(byteString)
      }
      
      responseObserver.onNext(replyBuilder.build());
      responseObserver.onCompleted();
    }

    override def relayQuery(req: Rpc.QueryRequest, responseObserver: io.grpc.stub.StreamObserver[Rpc.QueryReply]) = {
      var reply = Rpc.QueryReply.newBuilder().build();
      try {

        var query = req.getSqlQuery()
        query = query + "\n" + magicQuery;

        writer.write(query)
        writer.flush()

        var line: String = ""
        val replyBuilder = Rpc.QueryReply.newBuilder().setSuccess(true)
        while ((line = reader.readLine ()) != null && ! line.trim().equals(magicString)) {

          // We don't want to include duplicate lines from shell
          if (!line.startsWith("scala>")) { replyBuilder.addData(line)}
        }
        reply = replyBuilder.build();

      } catch {
        case _ : Throwable => println("Invalid Command")
        reply = Rpc.QueryReply.newBuilder()
	  .setSuccess(false)
	  .build()
      } finally {
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }

    override def relayFinishAttestation(req: Rpc.KeyRequest, responseObserver: io.grpc.stub.StreamObserver[Rpc.KeyReply]) = {

      val replyBuilder = Rpc.KeyReply.newBuilder()

      if (req.getNonNull()) {

        // Use name to create folder for client keys
        val name = req.getName()
        val keyDirectory = Files.createDirectory(tempDir.resolve("keys").resolve(name))
        
        // Create files for keys
        val keyNum = req.getKeyCount()
        
        var a = 0
        for (a <- 0 to keyNum - 1) {
          val msg = req.getKey(a).toByteArray()
          
          val keyFile = Files.createFile(Paths.get(keyDirectory.toString(), a.toString))
          Files.write(keyFile, msg)     
        }

        // Send command to shell to read key files and finish attestation
        // TODO: Complete this section
        var query = 
          """
          """

        writer.write(query)
        writer.flush()

        var line: String = ""
        val replyBuilder = Rpc.QueryReply.newBuilder().setSuccess(true)
        while ((line = reader.readLine ()) != null && ! line.trim().equals(magicString)) {
        }
              

	// Obtain key from message and store
        val reply = Rpc.KeyReply.newBuilder()
	  .setSuccess(true)
	  .build();
	responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } else {
	println("Failed to receive data or client did not confirm")
	val reply = replyBuilder
          .setSuccess(false) 
          .build();
	responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
  }
}
