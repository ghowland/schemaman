# SchemaMan


SchemaMan is a schema manager for multiple databases, so that schema confguration can be stored in meta data files, and applied equally to any database.

SchemaMan can be used as a generic wrapper for working with databases, or migrating data between databases, who have SchemaMan schemas defined.

The focus of SchemaMan is on smaller low-volume data sets, such as found in System or Network Configuration management systems.

## Version Management

Allows versioning data.

## Change Managament

Approvals can be required to commit data from a Working Version into the datasource.

## Security, Owners, and Actions

The Change Management can invoke scripts against data for owners, groups to provide security and proper approvals, and can invoke scripts as actions to perform pre- and post- commit work at various levels.

# Command Line Usage

```
Schema Actions:

  info                                       Print info on current schema directory
  init <path>                                Initialize a path for new schemas
  schema create <schema>                     Create a schema interactively
  schema export <schema> <source>            Export a database schema from a source
  schema update <schema> <source> <target>   Migrate schema/data from source to target
  data export <schema> <source>              Export all the data from the schema/source
  data import <schema> <source>              Import data into the schema/source

Data Actions:

  put <schema> <source> <json>        Put JSON data into a Schema instance
  get <schema> <source> <json>        Get Schema instance records from JSON keys
  filter <schema> <source> <json>     Filter Schema instance records
  delete <schema> <source> <json>     Delete records from Schema instance

Options:

  -d <path>, --dir=<path>             Directory for SchemaMan data/conf/schemas
                                          (Default is current working directory)
  -y, --yes                           Answer Yes to all prompts

  -h, -?, --help                      This usage information
  -v, --verbose                       Verbose output
```

