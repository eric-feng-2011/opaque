package edu.berkeley.cs.rise.opaque;

import py4j.GatewayServer;

import edu.berkeley.cs.rise.opaque.execution.SP;

object OpaqueClientSP {

  def main(args: Array[String]) {

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString
    val keyShare: Array[Byte] = "Opaque key share".getBytes("UTF-8")
    val clientKey: Array[Byte] = "Opaque key share".getBytes("UTF-8")

    Utils.addClientKey(clientKey)

    val sp = new SP()

    sp.Init(Utils.clientKey, intelCert, userCert, keyShare)

    val server = new GatewayServer(sp)

    server.start()
  }
}
