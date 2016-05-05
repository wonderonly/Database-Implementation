Requirement:
g++ is required to be able to compile.
-----------------------------------
The project is developed and tested under Ubuntu 15.10.
@auther: wonderonly@gmail.com
@date: Mon Apr 18 20:54:47 PDT 2016

-----------------------------------
Compile:
   $ make
Run:
   $ ./db
Exit:
   $ exit

-----------------------------------
Supported commands:
Commands are read from stdin.

1. load table:
   $ load table_name /path/to/file
    (note: multiple tables need to be loaded seperatedly.)

2. projecton:
   $ select col1 col2 from table
    (note: attribute names are seperated by space, no trailing ";".)

3. join:
   $ join inner table1 col1 on table2 col2
    (note: Keyword "join" is followed by join type keywords (inner/outer) then table name, attribute name, "on:, the other table name, and attribute name.
     Right now, only support two tables join and one join attribute for each side.)

4. arithemtic compute:
   $ compute col1 + col2 from table
    (note: '+' can be replaced with '-, *, or /'.)

5. aggregation:
   $ aggregation min col1 from table
    (note: 'min' can be replaced with 'max, average, or median'.)

-------------------------------------
Test case:
See 'sample.txt' for a complete set of examples on two csv files:
    'test1.csv' and 'test2.csv'.

Note for big data file:
    By default, all the results are printed to stdout. If tested with a big data file,please redirect stdout to file.

