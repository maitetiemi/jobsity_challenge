# JOBSITY CHALLENGE

## Requirements

---
* Python 3.9
* MySQL 5.8
* pyodbc, pandas, numpy, loggin, argparse
---

## Usage

---

`python etl_loader.py --jobsity -l logs\test_carga`

---

## Configuration

---
Configuration parameters are distribuited in several files under the folder config

### os_config

----
The Operating System main client configurable parameters are:

Parameter | Description
--------|-------------------------
`OS_ENVIROMENT` | Linux or Windows

### entity_config

---
Parameter | Description
--------|-------------------------
`DB_HOST` | Hostname of the database server
`DB_PORT` | Port where the database server listens
`DB_NAME` | Database Name
`USER_NAME` | Username to connect the database server
`DB_PASS` | Password of the user specified in user_name

---
### Motivation from this project
Execute a challenge for jobsity

### Games rule
[x] There must be an automated process to ingest and store the data.

[x] Trips with similar origin, destination, and time of day should be grouped together.

[x] Develop a way to obtain the weekly average number of trips for an area, defined by a
bounding box (given by coordinates) or by a region.

[x] Develop a way to inform the user about the status of the data ingestion without using a
polling solution.

[ ] The solution should be scalable to 100 million entries. It is encouraged to simplify the
data by a data model. Please add proof that the solution is scalable.

[x] Use a SQL database.


### Extract
Data will be extracted from the raw zone.
The files must be in CSV format, with a comma delimiter.
The script was programmed to read several files, I imaged one file per month, but if there is no separator per month, no problem.

### Transform
The transformations of this data are simple.
Eliminate duplication by origin, destination and day and region.
I convert the datetime column to timestamp and filter only the columns that matter

### Load
And finally I insert the data in a Mysql database.
The create table query is in the query folder.
Inserting data does not allow duplicates, so I created a unique key between source , destination and datetime

