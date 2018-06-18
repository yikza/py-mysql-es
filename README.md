py-mysql-es
========================

<a href="https://travis-ci.org/hikdo/py-mysql-es"><img src="https://travis-ci.org/hikdo/py-mysql-es.svg?branch=master"></a>

mysql binlog sync to elasticsearch


Installation
=============

```
 - pip install yaml
 - pip install pyelasticsearch
 - pip install mysql-replication
```

MySQL server settings
=========================

In your MySQL server configuration file you need to enable replication:

    [mysqld]
    server-id		 = 1
    log_bin			 = /var/log/mysql/mysql-bin.log
    expire_logs_days = 10
    max_binlog_size  = 100M
    binlog-format    = row

    