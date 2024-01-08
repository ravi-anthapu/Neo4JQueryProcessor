# Neo4J Query Analysys Tool

This tool assists with analyzing the query logs generated by Neo4j 
to understand the system peroformance in a more comprehensive manner.

This tool supports processing the JSON formatted logs downloaded from 
Neo4j Cloud database Aura and delimited formatted query logs generated by on-prem
installations of Neo4j. 

This tool processes the log files and writes the data to a SQLite database at 
this time. It can be enhanced to write to other datastores.

### Configuration

You need to provide a configuration yaml to process the data. These are the 
available configurations.

#### logType
There are 2 available options at this time. They are **aura** and **formatted**.

#### storeType
This the storage where the query details are written. At this time only **sqlite** is supported value

#### queryLocation
This points to the directory where the logs files are located.

#### fileFilter
This is an optional configuration. If this value is not provided it will 
process all the files in the query location directory. This takes a regular 
expression as an input.

Ex: _**^query.*\.log$**_

This expression reads all the files that start with **query** and have an extension **.log**. 

Say you want to process all the files that end with **json** extension, then the 
expression should be **_.*\\.json$_**

#### databaseURI
This takes the database URI. This is dependent on the storage type selected.

Ex: **_jdbc:sqlite:query_db.db_**

### Usage

1. Prepare a configuration yaml file.
2. Run the command **java -jar QueryProcessor.jar config.yaml**

A SQLite database will be created.

### Database tables
These are the database tables created in SQLite database.

#### queries
This table contains all the distinct queries along with runtime executed.

#### query_execution
This table contains the details for each of the query execution details.
It contains the query Id and transaction Id, start time, end time and all the 
details that are available related to planning, waiting, page hits, page faults
and time taken. It also captures the error details if they are available 
when the query fails to complete.