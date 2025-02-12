# Spring 2025

### Required Setup

* Java 21, Gradle, Kotlin. Linux and macOS users will love [SDKMAN!](https://sdkman.io) utility for managing Java and Gradle. It works on Windows as well, e.g. in [Git Bash](https://gitforwindows.org/)
* Git. 
* [Docker](https://www.docker.com/) -- it works on Linux and macOS and possibly on some editions of Windows as well.
* [PostgreSQL](https://www.postgresql.org/), preferrably [running in a Docker container](https://hub.docker.com/_/postgres).

### The goal and the process.

Our goal is to create a prototype of a distributed sharded replicated key-value storage (referred as KVaS hereafter).
We have a [basic demo implementation](./DEMO.md) that we need to extend by adding different sharding schemes, 
replication, consensus protocols and other stuff. 

We will have a few different KVaS implementations in different repositories. 
Our work consists of a few bi-weekly _sprints_. During a single sprint a team of two students will write and review code
of some particular KVaS extension. 

Please find a few moments to [read the guidelines](./CONTRIBUTION.md)


### Grading

The final grade is calculated from the coding assignment grades. Each assignment $A_i$ may give you up to $S_i$ credits (usually up to 10).
We will sum the gained credits, the sum of maximum possible credits and divide the former by the latter. 
