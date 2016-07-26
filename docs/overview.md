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

### Data Set: data/examples/opsdb/opsdb.yaml

```
# Can have multiple Data Sets per data set YAML file, they are the top level objects.
opsdb:
  name: OpsDB
  alias: opsdb
  owner_user: ghowland
  owner_group: ops
  
  # Specify the Data Sources
  datasource:
    # All of these database/user/password_path fields can be specified per host as well, which overrides these higher level defaults
    database: opsdb
    user: roleaccount
    password_path: data/examples/secure/opsdb_roleaccount.txt
    
    master_host_id: 1
    hosts:
      - id: 1
        host: somehostname.domain.com
        port: 3306
        type: mysql_56
  
  schema_paths:
    - data/examples/opsdb/schema/opsdb.yaml
  
  value_type_path: data/schema/value_types/standard.yaml
```


### data_source_types.yaml

```
mysql_56:
  name: MySQL 5.6
  handler: mysql
  
  value_type_path: data/schema/value_types/mysql/standard.yaml
```

### Example:  OpsDB data source schema:  data/examples/opsdb/schema/opsdb.yaml

```
# Table Schema for a Record
ip_address_v4:
  # Ordered Fields in the record
  - name: id
    type: pkey
  
  - name: ip_address
    type: ip_v4
    unique: true

ip_address_v6:
  id:
    type: pkey
  ip_address:
    type: ip_v6
    unique: true
```


### Value Types: data/schema/value_types/standard.yaml

```
pkey:
  name: Primary Key
  info: Auto-incrementing integer that specifies this record.

int:
  name: Integer
  info: Whole numbers

parent:
  name: Parent Record
  info: Points to a record in this table which is hierarchical parent to the current record.  Points at PKEY.

lookup:
  name: Lookup Record
  info: Points to a record in another table, specified in the schema definition.  Points at PKEY.

float:
  name: Float
  info: Floating point numbers.  Less precise than Decimal, but faster.

decimal:
  name: Decimal
  info: Decimal numbers.  Precise, without floating point less.

name:
  name: Name of Something
  info: Label or name of something.  A reasonably short text field.

info:
  name: Description of something.

text:
  name: Text
  info: A good amount of textual data, less than 64K.

longtext:
  name: Long Text
  info: More than 64K of textual data.

ip_v4:
  name: IP Address v4
  info: IPv4 Address

ip_v6:
  name: IP Address v6
  info: IPv6 Address

email:
  name: Email
  info: Email Address

phone:
  name: Phone Number

url:
  name: URL
  info: Universal Resource Locator

string:
  name: Short string.

blob:
  name: BLOB
  info: Binary large object.  Less than 64K.

longblob:
  name: Long BLOB
  info: Binary large object.  More than 64K.
```

### Data Source Value Types: data/schema/value_types/mysql/standard.yaml

```
pkey:
  type: INTEGER
  args: [11]
  options: ["PRIMARY KEY", "auto_increment"]

int:
  type: INTEGER
  args: [11]

parent:
  type: INTEGER
  args: [11]

lookup:
  type: INTEGER
  args: [11]

float:
  type: FLOAT
  args: [16, 5]

decimal:
  type: DECIMAL
  args: [16, 5]

name:
  type: VARCHAR
  args: [255]

info:
  type: TEXT

text:
  type: TEXT

longtext:
  type: LONGTEXT

ip_v4:
  type: VARCHAR
  args: [31]

ip_v6:
  type: VARCHAR
  args: [63]

email:
  type: VARCHAR
  args: [127]

phone:
  type: VARCHAR
  args: [63]

url:
  type: VARCHAR
  args: [511]

string:
  type: VARCHAR
  args: [511]

blob:
  type: BLOB

longblob:
  type: LONGBLOB
```
