# redoop
Redoop is a tool to help Data Architects to understand Relational Database workloads and make decisions based on data patterns.
It is been developed mainly to help offloading Oracle databases to Hadoop/Hive clusters

Install dependencies:
- cx_Oracle (> 5.2)
- Python 2.7.x


Usage:
./redoop.py -d oracle -o teste.txt

-d means database type (supported: Oracle)
-o output file

It generates a simple tabular text file with the result of specific querys.
Supported querys for Oracle:
* Analyze tables with full Scans
* Analyze tables subject to most DML
* Analyze cold range partition tables
