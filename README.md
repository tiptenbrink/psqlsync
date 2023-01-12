# PostgreSQLSync

Tool to create basic PostgreSQL dumps and restore them from local files. 

Don't use this for production-critical backups, SQL dumps (the method used by this library) are neither efficient nor safe for that purpose. Instead, use a tool like [Barman](https://pgbarman.org/).

### Based on [postgres_manage_python](https://github.com/valferon/postgres-manage-python) by [valferon](https://github.com/valferon). Thanks to him for the core logic.

This was forked to create a more minimal and maintainable package for the specific use case of syncing a populated testing database.

## Getting Started

### Setup

This library requires the installation of a PostgreSQL client, as it runs pg_restore and pg_dump directly from the command line, as there are no Python bindings for these functions, unfortunately. Use the below instructions to install the PostgreSQL 14 client ([instructions from here](https://wiki.postgresql.org/wiki/Apt)). Note that this installs some basic tools (see the first line of the shell below), replace them at your discretion if e.g. you don't want to bloat your container environment.

```shell
sudo apt install curl ca-certificates gnupg lsb-release
curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt update
sudo apt install postgresql-client-14
```

* Create configuration file (ie. sample.toml)
```toml
[setup]
storage_engine= "LOCAL"

[local_storage]
path = "./backups/"

[postgresql]
host="<your_psql_addr(probably 127.0.0.1)>"
port="<your_psql_port(probably 5432)>"
db="<your_db_name>"
user="<your_username>"
password="<your_password>"
```


### Usage

* List databases on a PostgreSQL server

      psqlsync --config sample.toml --action list_dbs

* Create database backup and store it (based on config file details)

      psqlsync --config sample.toml --action backup --verbose

* List previously created database backups available on storage engine

      psqlsync --config sample.toml --action list

* Restore previously created database backups available on storage engine (check available dates with *list* action, it matches the time string, so any unique part of the string suffices)

      pslsync --config sample.toml --action restore --date 2021

* Restore previously created database backups into a new destination database

      pslsync --config sample.toml --action restore --date 20211219-14 --dest-db new_DB_name

* Enter password in prompt, so it does not have to be stored in plaintext in the config file

      pslsync --config sample.toml --action backup --prompt-pass
      Password for database: 


### Command help
```
usage: psqlsync [-h] --action action [--time YYYYMMdd-HHmmss] [--dest-db dest_db] [--verbose] --config CONFIG
                [--prompt-pass]

psqlsync

optional arguments:
  -h, --help            show this help message and exit
  --action action       'list' (backups), 'list_dbs' (available dbs), 'restore' (requires --time), 'backup'
  --time YYYYMMdd-HHmmss
                        Time to use for restore (show with --action list). If unique, will smart match. (If
                        there's just one backup matching YYYMM, providing that is enough)
  --dest-db dest_db     Name of the new restored database
  --verbose             Verbose output
  --config CONFIG       Database configuration file path (.toml)
  --prompt-pass         Show a password prompt instead of the password defined in the config.
```


### Run programmatically

The `backup` and `restore` action have been seperated into easily callable Python functions in `psqlsync.actions`. You can import this module and call these functions from your Python code.


## Authors

* **Tip ten Brink**
* **[Val Feron](https://github.com/valferon)** - *Initial work* 


## License

The original code, created by [valferon](https://github.com/valferon) in the [postgres_manage_python repository](https://github.com/valferon/postgres-manage-python), is licensed under the MIT License. This project as a whole, most notably my original code, is licensed under the Apache License v2.0.
