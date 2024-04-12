# Persistence

This repository contains the source code for the
UNICORE persistence library, a simple utility for
storing data objects in different database backends.

Supports H2, MySQL and PostgreSQL.


## Integration tests

Integration tests are run automatically via GitHub actions.

To run the integration tests locally, start a MySQL container using

```
docker run --rm --name mysql-test -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -d mysql:8.3.0
# create a 'test' database
docker exec mysql-test mysql -u root --password='root' -e "CREATE DATABASE test;"
```

and a PostGreSQL container

```
docker run --rm --name pgsql-test -p 5432:5432 -e POSTGRES_PASSWORD=root -d postgres:16.2
```

(note that a few moments are needed for the databases to become fully operational)


Launch the integration tests via

```
mvn test -Pintegrationtest
```
