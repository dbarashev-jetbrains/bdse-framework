package kvas.loadtest

import io.grpc.ManagedChannelBuilder
import kvas.proto.KvasGrpc
import kvas.proto.replicaGetGroupRequest
import kvas.util.toHostPort
import kotlin.random.Random

class ReplicaRouter(private val primaryAddress: String) {
  private val replica2stub = mutableMapOf<Int, KvasGrpc.KvasBlockingStub>()
  private val address2shardNumber = mutableMapOf<String, Int>()
  init {
    newStub(primaryAddress).replicaGetGroup(replicaGetGroupRequest { }).also {
      println("replicas: $it")
      it.replicasList.forEachIndexed {idx, replica ->
        replica2stub[idx] = newStub(replica.nodeAddress)
        address2shardNumber[replica.nodeAddress] = idx
      }
    }
  }

  fun address2shardNumber(address: String) =
    address2shardNumber[address]

  fun replaceShard(shardNum: Int) = Random.nextInt(replica2stub.size)
  fun replicaLeaderNumber(key: String) = address2shardNumber[primaryAddress] ?: 0
  fun replicaRandomNumber(key: String) = Random.nextInt(replica2stub.size)
  fun getStub(shardToken: Int): KvasGrpc.KvasBlockingStub? = replica2stub[shardToken]

  fun newStub(address: String) = address.toHostPort().let {
    KvasGrpc.newBlockingStub(ManagedChannelBuilder.forAddress(it.first, it.second).usePlaintext().build())
  }
}