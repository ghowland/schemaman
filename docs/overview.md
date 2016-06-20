# Overview

 * Create and Update Schema
 * Import/Export Schema
 * Import/Export Data
 * Get/Put Data
 * Filter Data, Delete based on Filter
 * Version Managament and Change Management (VMCM)
 * Run scripts to handle: Security, Ownership/Authorization, Data-Oriented Actions

# Schemas

## Definitions

 * **Data Set** - The big-picture of a set of data.  In normal parlance this would be a "database" but in SchemaMan parlance, a "database" is 2 levels down:  Data Set -> Data Source -> Database.  This puts **Data Set** at the very top of a hierarchy when talking about data.  The Data Set is the total data being looked at together, which may be stored in multiple types of databases (MySQL, Postrgres, Cassandra, SQLite, etc), all of which may be using the same schemas (for their perspective storage engines).
 * **Data Source** - Datasource is the level beneath the **Data Set**, which specifies a type of database system being used (MySQL, Postgrel, Cassandra, etc) and any connection specifics, such as the host, port, username, etc, which are all required for accessing a data source.  There can be multiple datasources which are the same except for the user connecting to them, which gives a different view into the data due to authorization differences.
 * **Data Source Type** - MySQL, Postgres, Cassandra, SQLite, etc.  This is the type of Data Source being used.  Version will be included, as different versions may not be compatible with each other.
 * **Database** - This is inside a **Data Source**, and is the container in which **Data Tables** or equivalent storage of records of data occurs.  
 * **Data Table** - This is what **Records** are stored in.
 * **Record** - A collection of fields.  This is a group of data, and is stored as a Row of Fields in some Data Source Types, and Column of Fields in others, and other ways as well.  This is the structure where fields are grouped together.
 * **Field** - A single type of data.  Given a Value Type.
 * **Value Type** - The type of a value for a Field, such as "Text", "Long Text" or "Decimal".  This is a SchemaMan definition, and will be related to specified Data Source Type definitions such as VARCHAR, TEXT and DECIMAL, etc.
 * **Data Source Value Type** - The specific implemetation of a Value Type in a Data Source.

## Specifications

### dataset_opsdb.yaml

```
# Can have multiple Data Sets per data set YAML file, they are the top level objects.
opsdb:
  name: OpsDB
  owner_user: ghowland
  owner_group: ops
  
  datasource:
    type: mysql_56
    hosts:
      - id: 1
        host: somehostname.domain.com
        port: 3306
    
    master_host_id: 1
    
    database: opsdb
    
    user: roleaccount
    password_path: /Users/ghowland/secure/datasource/opsdb_roleaccount.txt
  
  record_path: /Users/ghowland/projects/schema/opsdb/opsdb.yaml
```


### datasourcetype.yaml

```
mysql_56:
  name: MySQL 5.6
  handler: mysql
```

