# SchemaMan


SchemaMan is a schema manager for multiple databases, so that schema confguration can be stored in meta data files, and applied equally to any database.

SchemaMan can be used as a generic wrapper for working with databases, or migrating data between databases, who have SchemaMan schemas defined.

The focus of SchemaMan is on smaller low-volume data sets, such as found in System or Network Configuration management systems.

## Version Management

Allows versioning data.

## Change Managament

Approvals can be required to commit data from a Working Version into the datasource.

## Definitions

 * **VMCM** - Version Management and Change Management system
 * **Client-Service** - A service (ex: mid-tier) that connects to ChangeIn to handle VMCM
 * **Client-Service User** - The user of a Client-Service, which will use the Client-Service directly, but whose authentication information is passed along to ChangeIn to identify their Change Sets.  This might be a human user, or role user, but must be authenticated and have the proper authorization determined by the Client-Service's configuration.
 * **User** - Same as Client-Service User. 
 * **Client-Service Configuration** - The ChangeIn specific configuration for a given Client-Service.
 * **Working Change Set** - A list of record fields that a Client-Service User is making edits to.  These are not applied to any live datasource, and are not integration until all the edits are finished, a Pending Change Set is created, for review.
 * **Pending Change Set** - A list of record fields that a Client-Service User has specified as edits in a Working Change Set, and promoted to a Pending Change Set for review.  The approval process starts here.
 * **Approved Change Set** - A list of record fields that a Client-Service User promoted to a Pending Change Set, and then all required approvers marked "Approved" moving the state from Pending to Approved.  The next step is Deployed Change Set.
 * **Deployed Change Set** - A list of record fields that has been approved.  A Client-Service will need to trigger a Deployment of the changes, which is when the changes are actually applied into the Client-Service's data sources.  Before this point, no data changes were made to the Client-Service's data sources.  This write must be done by the Client-Service itself, as ChangeIn has no knowledge of how this would happen or authorization to perform this work.  Call backs should be implemented for pre- and post- deployment scripts, so that they stages can be processed in ChangeIn, tracking the progress of the work.
 * **Committed Change Set** - A list of record fields that has been deployed and written into a Client-Service's data sources.  The committed data is kept in ChangeIn to provide auditing functionality, and also for retrieval for use in Roll Backs.
 * **Collected Change Set** - A list of record fields that was either rejected or otherwise brought out of the approval process, but is still collected together for editing purposes.
 * **Approval Group** - All record field changes have an Approval Group consisting of 1 or more Approvers, which is provided by the Client-Service at Working Change Set update or before the Pending Change Set promotion through a call to the Client-Service API which takes all the record field changes and returns the list of Approval Groups and Approvers in the ChangeIn specified format.
 * **Approvers** - Members of an approval group.  Human or role accounts.  This could also be a Service based authorization, through API call-back.

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

