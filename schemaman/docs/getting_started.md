# Getting Started

## Creating a configuration from an existing database

### 0. Login as the user who should own the SchemaMan configuration files

### 1. Create a directory where new schema files will be created, and set it into a variable we can use

```
SCHEMA_DIR=/some/directory/for/the/schema/defintions/to/live/in

mkdir -p $SCHEMA_DIR
```

### 2. Run SchemaMan from the Command Line to create required YAML paths

```
./schemaman.py init $SCHEMA_DIR
```

### 3. Export an existing database into this schema

```
./schemaman.py schema export $SCHEMA_DIR
```

### 4. Edit the exported schema to change the schema record field types to SchemaMan versions of them

The benefit of using SchemaMan abstracted types is that they can move between different Data Source Type backends (MySQL, Cassandra, etc), using the native types of each database backend, while working with a compatible but abstracted data type between all of them.

```
vi $SCHEMA_DIR/schema/*.yaml
```

### 5. Update the schema again with your changes, which will change any field sizes who have been mapped to our abstracted database value types.

```
./schemaman.py schema update $SCHEMA_DIR
```

### 6. Start using your database through SchemaMan

