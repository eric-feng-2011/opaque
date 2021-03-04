package edu.berkeley.cs.rise.opaque

import com.google.protobuf.ByteString

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import rpc.Rpc
import rpc.OpaqueRPCGrpc
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import scala.io.StdIn.readLine

import edu.berkeley.cs.rise.opaque.execution.SP

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldClient.java]]
 */
object OpaqueClient {

  def apply(host: String, port: Int): OpaqueClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = OpaqueRPCGrpc.newBlockingStub(channel)

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString
    val keyShare: Array[Byte] = "Opaque key share".getBytes("UTF-8")
    val clientKey: Array[Byte] = "Opaque key share".getBytes("UTF-8")

    Utils.addClientKey(clientKey)

    val sp = new SP()

    sp.Init(Utils.clientKey, intelCert, userCert, keyShare)

    new OpaqueClient(channel, blockingStub, sp)
  }

  // Shell reads user input one line at a time
  def shell(client: OpaqueClient): Unit = {
    while (true) {

      // Comment the line out below if using with sbt
//      print("opaque> ")

      val test = readLine()
      client.sendQuery(test)
    }
  }

  def main(args: Array[String]): Unit = {
    val client = OpaqueClient("localhost", 50051)
    try {
      val user = args.headOption.getOrElse("world")
//      client.getRA(user)
//      client.sendTestQuery()
    } finally {
    }

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC client")
      client.shutdown()
      System.err.println("*** client shut down")
    }

    shell(client)
  }
}

class OpaqueClient private(
  private val channel: ManagedChannel,
  private val blockingStub: OpaqueRPCGrpc.OpaqueRPCBlockingStub,
  private val serviceProvider: SP
) {
  private[this] val logger = Logger.getLogger(classOf[OpaqueClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def getRA(name: String): Unit = {
    val request = Rpc.RARequest
	.newBuilder()
	.setName(name)
	.build()
    try {
      val response = blockingStub.relayGenerateReport(request)
      if (response.getSuccess()) {
	// Verify the RA report is correct and return the client key if so
        val msg2 = serviceProvider.ProcessEnclaveReport(response.getReport().toByteArray())
        val byteString: ByteString = ByteString.copyFrom(msg2)

	// Building the key response
        val request2 = Rpc.KeyRequest.newBuilder()
	  .setNonNull(true)
	  .setKey(byteString)
	  .build()
        val response2 = blockingStub.relayFinishAttestation(request2)
        if (response2.getSuccess()) {
          println("Attestation passes")
        }
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  def sendTestQuery(): Unit = {
    val testQuery = 
      """
val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
val df = spark.createDataFrame(data).toDF("word", "count")
df.show()
      """
    val request = Rpc.QueryRequest.newBuilder()
      .setSqlQuery(testQuery)
      .build()
    val response = blockingStub.relayQuery(request)
    var a = 0
    for (a <- 0 to (response.getDataCount() - 1)) {
      println(response.getData(a))
    }
  } 

  def sendQuery(query: String): Unit = {
    val request = Rpc.QueryRequest.newBuilder()
      .setSqlQuery(query)
      .build()
    val response = blockingStub.relayQuery(request)
    var a = 0
    for (a <- 0 to (response.getDataCount() - 1)) {
      println(response.getData(a))
    }
  }
}

