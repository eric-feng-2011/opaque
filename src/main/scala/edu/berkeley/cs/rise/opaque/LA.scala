/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import edu.berkeley.cs.rise.opaque.execution.SP
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave

// Helper to handle enclave "local attestation" and determine shared key

object LA extends Logging {
  def initLA(sc: SparkContext): Unit = {

    val rdd = sc.makeRDD(Seq.fill(sc.defaultParallelism) { () })

    // Test print Utils.
    println(Utils)

    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
     
      // Print utils and enclave address to ascertain different enclaves
      println(Utils)
      println(eid)

      val msg1 = enclave.GetPublicKey(eid) 
      Iterator((eid, msg1))
    }.collect.toMap

   println("Finish LA")

//    val msg2s = msg1s.map{case (eid, msg1) => (eid, sp.ProcessEnclaveReport(msg1))}
//    msg1s.map{case (eid, msg1) => (eid, print(eid + "\n"))}
//
//    val attestationResults = rdd.mapPartitionsWithIndex { (_, _) =>
//      val (enclave, eid) = Utils.initEnclave()
//      enclave.FinishAttestation(eid, msg2s(eid))
//      Iterator((eid, true))
//    }.collect.toMap
//
//    for ((_, ret) <- attestationResults) {
//      if (!ret)
//        throw new OpaqueException("Attestation failed")
//    }
  }
}
