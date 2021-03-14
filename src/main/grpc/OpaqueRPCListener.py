from concurrent import futures

import grpc

import os
import sys, getopt
import atexit
from pexpect import replwrap

import rpc_pb2
import rpc_pb2_grpc

# Remove the first line of the output which is a repeat of the input
def clean_shell_output(output):
  parsed_output = output.split("\n",1)[1]
  return parsed_output

class OpaqueRPCListener(rpc_pb2_grpc.OpaqueRPCServicer):

  def __init__(self):

    spark_home = os.getenv("SPARK_HOME")
    opaque_jar = ''
    master = ''

    # Determine opaque jar and ip of master
    argv = sys.argv[1:]
    try:
      opts, args = getopt.getopt(argv,"hj:m:",["jar=","master="])
    except getopt.GetoptError:
      print('OpaqueRPCListener.py -j <opaque jar> -m <master cluster ip>')
      sys.exit(2)
    for opt, arg in opts:
      if opt == '-h':
         print('OpaqueRPCListener.py -j <opaque jar> -m <master cluster ip>')
         sys.exit()
      elif opt in ("-j", "--jar"):
         opaque_jar = arg
      elif opt in ("-m", "--master"):
         master = arg

    # TODO: Currently hardcoding memory capacities. Will need to change to be dynamic.
    spark_shell_bin = spark_home +  "/bin/spark-shell" + " --jars " + opaque_jar + " --master" + master\
      + "--driver-memory 1G --executor-memory=2G"

    self.proc = replwrap.REPLWrapper(spark_shell_bin, "scala> ", prompt_change=None)
    print("Instantiate subprocess")

    self.proc.run_command("import edu.berkeley.cs.rise.opaque.implicits._")
    self.proc.run_command("edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)")
    print("Imported Opaque libraries")

  def relayGenerateReport(self, request, context):
    output = self.proc.run_command("print_report")
    clean_output = clean_shell_output(output)
    reply = rpc_pb2.RAReply(success = True, report = clean_output)
    return reply

  ''' TODO: Do not need repeated field anymore. Can remove and only send string back.
  Other things that I can add for a better shell:
  1. Handle multi-line input
  2. Allow for up/down arrow of input (this should be done on client side)
  '''
  def relayQuery(self, request, context):

    if not request.query: 
      return rpc_pb2.QueryReply(success = True, data = "")

    output = self.proc.run_command(request.query)
    clean_output = clean_shell_output(output)
    reply = rpc_pb2.QueryReply(success = True, data = clean_output)
    return reply

  def relayFinishAttestation(self, request, context):
    print("Hello World - Finish Attestation")

def clean_up():
  print("Server shutoff")

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  rpc_pb2_grpc.add_OpaqueRPCServicer_to_server(OpaqueRPCListener(), server)
  server.add_insecure_port('[::]:50051')
  server.start()
  server.wait_for_termination()
  print("Hello world - serve")

if __name__ == '__main__':
    atexit.register(clean_up)
    serve()