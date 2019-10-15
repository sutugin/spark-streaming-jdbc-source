
# This repository contains custom jdbc source for spark structured streaming 

- All parameters are set as in a normal, non-streaming, JDBC connection (https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
except that the option must add the property "offsetColumn", 
indicating the name of the column on which the offset will be taken(the column must be a number/date/ timestamp).
 
- See the tests for usage examples.

- ToDo: Support other data types for offsets.