import grpc
import atexit

import rpc_pb2
import rpc_pb2_grpc

#Used for ServiceProvider
from py4j.java_gateway import JavaGateway

gateway = JavaGateway()
SP_app = gateway.entry_point

def perform_ra(stub):
    response = stub.relayGenerateReport(rpc_pb2.RARequest(name = "user"))
    key_request = rpc_pb2.KeyRequest(name = "user", success = True)

    # TODO: Since we are obtaining all the reports at once, I will need to parse the values to obtain each
    # report individually
    try:
      for report in response.report:
          msg = SP_app.ProcessEnclaveReport(response.report)
          key_request.append(msg)
    except:
      print("Failure in verifying enclave reports")

    response = stub.relayFinishAttestation(key_request)
    if response.success:
        print("Attestation successful")

def send_query(stub, query):
    response = stub.relayQuery(rpc_pb2.QueryRequest(query=query))
    print(response.data)

def shell(stub):
    while True:
        user_input = input("opaque> ")
        send_query(stub, user_input)

def clean_up(channel):
    channel.close()
    print("Channel closed")

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = rpc_pb2_grpc.OpaqueRPCStub(channel)

    # TODO: Ask if should start the subprocess that is the java serviceprovider
    # Currently starting manually

    atexit.register(clean_up, channel=channel)

    # Perform ra
    perform_ra(stub)

    shell(stub)

if __name__ == '__main__':
    run()
