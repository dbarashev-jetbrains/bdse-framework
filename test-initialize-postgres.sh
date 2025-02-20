#!/bin/bash

echo "Removing existing PostgreSQL containers..."
docker rm -f kvas-postgres-0 && docker rm -f kvas-postgres-1 && docker rm -f kvas-postgres-2

echo "Starting three PostgreSQL containers..."
docker run -d -p 5430:5432 --name kvas-postgres-0 -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker run -d -p 5431:5432 --name kvas-postgres-1 -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker run -d -p 5432:5432 --name kvas-postgres-2 -e POSTGRES_HOST_AUTH_METHOD=trust postgres

sleep 5
echo "Initializing PostgreSQL..."
psql -h localhost -U postgres -p 5430 -f kvnode/src/main/resources/postgres-init.sql
psql -h localhost -U postgres -p 5431 -f kvnode/src/main/resources/postgres-init.sql
psql -h localhost -U postgres -p 5432 -f kvnode/src/main/resources/postgres-init.sql

