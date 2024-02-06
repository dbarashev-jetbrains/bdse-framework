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


