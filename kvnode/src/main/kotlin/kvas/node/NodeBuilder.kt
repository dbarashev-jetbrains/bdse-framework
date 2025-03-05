package kvas.node

import io.grpc.ServerBuilder
import kvas.node.replication.LeaderlessReplication
import kvas.node.replication.LeaderlessReplicationDataServiceImpl
import kvas.node.replication.ReplicationFollowerFactory
import kvas.node.replication.ReplicationLeaderFactory
import kvas.proto.KvasProto
import kvas.setup.SingleShard

internal fun KvasNodeBuilder.buildReplicationNode(grpcBuilder: ServerBuilder<*>) {
    if (this.replicationConfig.role == "leaderless") {
        val failingStorage = this.createFailingStorage(this.storage)
        val leaderlessNode = LeaderlessReplication.ALL[this.replicationConfig.impl]!!.invoke(failingStorage)
        this.storage = leaderlessNode.coordinatorStorage
        val statisticsStorage = StatisticsStorage(this.storage)

        val dataService = LeaderlessReplicationDataServiceImpl(leaderlessNode)
        grpcBuilder.addService(dataService)
        grpcBuilder.addService(StatisticsService(statisticsStorage))
        metadataListeners.add(leaderlessNode.metadataListener)

        RegisterTask(
            metadataStub, selfAddress,
            if (replicationConfig.isFollower) KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
            else KvasProto.RegisterNodeRequest.Role.LEADER_NODE,
            { 0 },
            leaderlessNode::onRegister
        )
    } else {
        if (this.replicationConfig.isFollower) {
            val replicationFollower = ReplicationFollowerFactory.ALL[this.replicationConfig.impl]!!.invoke(
                this.selfAddress,
                storage,
                metadataStub
            )
            grpcBuilder.addService(replicationFollower.createGrpcService())
            this.storage = replicationFollower.storage
        } else {
            val replicationLeader = ReplicationLeaderFactory.ALL[this.replicationConfig.impl]!!.invoke(
                this.selfAddress,
                storage,
                metadataStub
            )
            this.storage = replicationLeader.createStorage()
            this.metadataListeners.add(replicationLeader.createMetadataListener())
        }
        val statisticsStorage = StatisticsStorage(this.createFailingStorage(this.storage))
        val dataService = KvasDataNode(
            selfAddress = this.selfAddress, storage = statisticsStorage,
            sharding = SingleShard,
            dataTransferProtocol = DataTransferProtocols.DEMO.second.invoke(
                SingleShard,
                selfAddress,
                storage
            ),
        )
        grpcBuilder.addService(dataService)
        metadataListeners.add(dataService::onShardingChange)
        grpcBuilder.addService(dataService.createDataTransferService())
        grpcBuilder.addService(StatisticsService(statisticsStorage))
        RegisterTask(
            metadataStub, selfAddress,
            if (replicationConfig.isFollower) KvasProto.RegisterNodeRequest.Role.REPLICA_NODE
            else KvasProto.RegisterNodeRequest.Role.LEADER_NODE,
            { dataService.shardToken },
            dataService::onRegister
        )
    }
}

internal fun KvasNodeBuilder.buildShardingNode(grpcBuilder: ServerBuilder<*>) {
    val statisticsStorage = StatisticsStorage(this.createFailingStorage(this.storage))
    val dataService = KvasDataNode(
        selfAddress = this.selfAddress, storage = statisticsStorage,
        sharding = this.sharding,
        dataTransferProtocol = DataTransferProtocols.ALL[this.dataTransferServiceImpl]!!.invoke(
            sharding,
            selfAddress,
            storage
        ),
    )
    RegisterTask(metadataStub, selfAddress, KvasProto.RegisterNodeRequest.Role.LEADER_NODE, { dataService.shardToken }, dataService::onRegister)
    grpcBuilder.addService(dataService)
    metadataListeners.add(dataService::onShardingChange)
    grpcBuilder.addService(MetadataListenerImpl { shardingChangeRequest ->
        metadataListeners.forEach { it.invoke(shardingChangeRequest.metadata) }
    })
    grpcBuilder.addService(dataService.createDataTransferService())
    grpcBuilder.addService(StatisticsService(statisticsStorage))

}