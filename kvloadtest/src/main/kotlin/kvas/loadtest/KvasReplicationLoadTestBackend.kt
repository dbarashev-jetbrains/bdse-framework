package kvas.loadtest

import io.grpc.ManagedChannelBuilder
import kvas.proto.KvasGrpc
import kvas.proto.replicaGetGroupRequest
import kvas.util.toHostPort
import kotlin.random.Random

class ReplicaRouter(primaryAddress: String) {
  private val replica2stub = mutableMapOf<Int, KvasGrpc.KvasBlockingStub>()
  init {
    newStub(primaryAddress).replicaGetGroup(replicaGetGroupRequest { }).also {
      println("replicas: $it")
      it.replicasList.forEach {replica ->
        replica2stub[replica.shardToken] = newStub(replica.nodeAddress)
      }
    }
  }

  fun replicaNumberPut(key: String) = 0
  fun replicaNumberGet(key: String) = Random.nextInt(replica2stub.size)
  fun getStub(shardToken: Int): KvasGrpc.KvasBlockingStub? = replica2stub[shardToken]

  fun newStub(address: String) = address.toHostPort().let {
    KvasGrpc.newBlockingStub(ManagedChannelBuilder.forAddress(it.first, it.second).usePlaintext().build())
  }
}