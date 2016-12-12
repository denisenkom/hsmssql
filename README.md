hsmssql
=======

Pure Haskell MSSQL driver

Status
------

Works with MSSQL 2008


Running Tests
-------------

First install dependencies:

```bash
 cabal install --enable-tests
```

Setup environment variables:

- **HOST (required)** - host name where SQL server listens
- INSTANCE (optional) - instance name of SQL server
- SQLUSER (optional) - user name to connect under, default is *sa*
- SQLPASSWORD (optional) - password, default is *sa*
 
To run tests run:

```bash
 cabal test
```
