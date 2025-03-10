Start PostgreSQL instance:

```shell
docker run -d -p 5432:5432 --name primary -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker run -d -p 5431:5432 --name replica --link primary:primary -e POSTGRES_HOST_AUTH_METHOD=trust postgres
```


createuser -U postgres --replication repka

echo "host replication all 172.17.0.0/16 trust" >> /var/lib/postgresql/data/pg_hba.conf