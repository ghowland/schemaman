# TODO

 * Version management from SETs, from specified user, against head
 * Head GET with user specific data gets that data merged into the result, because its current version for that user.  They use the Head actual version number to get raw data, unchanged.
 * Head and versioned data from GETs
 * Change Management action to COMMIT
 * Populate actions for basic security, ownership and doing change management

# Done 
 
 * Connect from schema defintion, holding cursors in the datasource/__init__
 * Pull down schema into a YAML file.  SHOW TABLES, DESC %s, pull out fields and args and stuff
 * Put schema into DB tables, based on another script.  Action?
 * Configure version and change management in Schema and DB
 * Request object wraps all user authentication and connection specification and any additional request oriented data

