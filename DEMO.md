# Demo implementation

The demo implementation is a sharded KV storage that keeps data in-memory and uses a naive hashing scheme, where the 
shard number is determined by dividing the hash code by the total number of shards.

The demo implementation also provide a simple metadata server.

### Quickstart

In order to see a working KVAS instance, you need to launch a KVAS server with the metadata and data node functions:

```bash
gradle :kvnode:run --args='--storage memory metadata --master'
```
  
By default, the server listens on port 9000. The server command line arguments can be printed with: `gradle :kvnode:run --args='--help'`
  
To test the server, run a KVAS client that issues a simple PUT command and then run it with a simple GET command: 

```bash
gradle :kvclient:run --args='put foo bar'
gradle :kvclient:run --args='get foo'
```

It will connect to the metadata server at localhost:9000, will
fetch a list of available shards and will send a request to the shard that corresponds to the naive sharding scheme. 
  In the trivial case when there is just one data node, there will be a single shard. 

You can also run a KVAS client in the shell mode where you can issue GET and PUT commands many times:

  `gradle :kvclient:run --args='shell'`

### Local multi-node setup 

If you need to run multiple KVAS servers on your local machine in a single cluster, you need to:

- assign a different port to every instance using `--grpc-port` command-line argument.
- let them know the master node address (IP:PORT) using `metadata --address` command line argument.

An example of running a cluster with a master node and one worker may look as follows: 
```shell
# kvnode with metadata server functions
gradle :kvnode:run --args='--storage memory metadata --master'

# second shard/data node
gradle :kvnode:run --args='--grpc-port 9001 --storage memory metadata --address localhost:9000'
```

### Running local load test
You can run a load test that sends a load of PUT and GET request to any KVAS cluster which is accessible by IP address.
The command looks like:

```shell
gradle :kvclient:run --args='--metadata-address localhost:9000 --sharding naive loadtest --key-count 10000 --client-count 2'
```

The command given above will start two client threads and will send 10000 pairs of PUT/GET requests to the cluster with metadata
server running at `localhost:9000`. At the end, it will print the resulting number of Queries Per Second (QPS)

