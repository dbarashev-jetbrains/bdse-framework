# Spring 2024

### Required Setup

* Java 17, Gradle, Kotlin. Linux and macOS users will love [SDKMAN!](https://sdkman.io) utility for managing Java and Gradle. It works on Windows as well, e.g. in [Git Bash](https://gitforwindows.org/)
* Git. 
* [Docker](https://www.docker.com/) -- it works on Linux and macOS and possibly on some editions of Windows as well.
* [PostgreSQL](https://www.postgresql.org/), preferrably [running in a Docker container](https://hub.docker.com/_/postgres).
* [Terraform](https://www.terraform.io/) for cloud deployment

### Ultimate goal

Implement a prototype of a distributed sharded replicated key-value storage with the code name "kvas" (stands for Key-VAlue Storage)

### Rules

* [How to create a pull request](./CONTRIBUTION.md)

### Stub implementation

The stub implementation is an echo server that responds to getValue requests and for each requested key returns the key itself, unless it is 42.


* Running a KVAS server:

    `gradle :kvnode:run`
  
  By default, the server listens on port 9000 and when it starts, it tries to connect to PostgreSQL server on localhost:5432 using `postgres` username and empt password. It is not required for the echo server operations, but it just checks if PostgreSQL is running.

The server command line arguments can be printed with:
  `gradle :kvnode:run --args='--help'`
  
* Running PostgreSQL in a Docker container:
    
    `docker run -d -p 5432:5432 --name kvas-postgres -e POSTGRES_HOST_AUTH_METHOD=trust postgres`

    It will start with `postgres` user and empty password (that corresponds to the default KVAS server config)

* Running KVAS client:

  `gradle :kvclient:run --args='get foo'`

  By default the client talks to the server running on `localhost:9000`


### Local multi-node setup 

If you need to run multiple KVAS servers on your local machine in a single cluster, you need to:

- assign a different port to every instance using `--grpc-port` command-line argument.
- let them know the master node address (IP:PORT) using `--master` command line argument.
- optionally let every instance know its own full address (IP:PORT) using `--self-address` argument. This is a technically not necessary when they run outside of docker containers, because they will use `127.0.0.1:<grpc-port>` by default, however it will be necessary if KVAS is running in a Docker container, where "localhost" or "127.0.0.1" resolves inside the container itself.

Every kvnode instance will most likely need its own PostgreSQL storage. 

An example of running a cluster with a master node and one worker, talking to their own PostgreSQL instances may look as follows: 
```shell
# PostgreSQL storage for the master node
docker run -d -p 5430:5432 --name postgres-0 -e POSTGRES_HOST_AUTH_METHOD=trust postgres

# PostgreSQL storage for the worker node
docker run -d -p 5431:5432 --name postgres-1 -e POSTGRES_HOST_AUTH_METHOD=trust postgres

# master kvnode
gradle :kvnode:run --args='--grpc-port 9000 --db-port 5430'

# worker kvnode
gradle :kvnode:run --args='--grpc-port 9001 --db-port 5431 --master 127.0.0.1:9000'
```

### Running local load test
You can run a load test that sends a load of PUT and GET request to any KVAS cluster which is accessible by IP address.
The command looks like:

```shell
# gradle :kvloadtest:run --args='sharding --help'
> Task :kvloadtest:run
Usage: main sharding [<options>]

  sharding

Options:
  --http-port=<int>                    (default: 0)
  --kvas-address=<text>                (default: 127.0.0.1:9000)
  --key-count=<int>                    (default: 10)
  --connection-count=<int>             (default: 1)
  --method=(linear|simple|consistent)  (default: linear)
  -h, --help                           Show this message and exit
```

The load test client is aware of three sharding methods. You can find their implementations in `KvasLoadTestBackend.kt` file.
If the value of `--http-port` argument is other than 0, the load test starts an HTTP server on the specified port and ignores
the remaining arguments. It waits for HTTP requests to its `/` path with the load test parameters specified in the query arguments: 

```shell
curl localhost:8080?master=127.0.0.1:9000&connections=4&keys=10000&method=linear
```