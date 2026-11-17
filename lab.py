"""
Big Data Laboratory Manual - Reference Code & Exam Practice
Ubuntu / HDFS / MapReduce / MongoDB / Pig / Hive
"""

import sys

PROGRAM_1 = r"""================================================================================
PROGRAM 1: HDFS FILE MANAGEMENT TASKS
================================================================================

1. Start Hadoop Daemons:
start-all.sh
jps

2. Create Local File & HDFS Directory:
echo "Welcome to Big Data Lab - Program 1 Test" > test_file.txt
hadoop fs -mkdir -p /demo_dir
hadoop fs -ls /

3. Upload File to HDFS:
hadoop fs -put test_file.txt /demo_dir/
hadoop fs -ls /demo_dir

4. Retrieve and Read File from HDFS:
hadoop fs -cat /demo_dir/test_file.txt
hadoop fs -get /demo_dir/test_file.txt retrieved_file.txt
cat retrieved_file.txt

5. Delete File and Directory from HDFS:
hadoop fs -rm /demo_dir/test_file.txt
hadoop fs -rm -r /demo_dir
hadoop fs -ls /
"""

PROGRAM_2 = r"""================================================================================
PROGRAM 2: MAPREDUCE MATRIX MULTIPLICATION
================================================================================

1. Create Input Matrix Files:
cat << 'EOF' > matrix_a.txt
A 0 0 1
A 0 1 2
A 1 0 3
A 1 1 4
EOF

cat << 'EOF' > matrix_b.txt
B 0 0 5
B 0 1 6
B 1 0 7
B 1 1 8
EOF

2. Create Mapper Script (mapper.py):
cat << 'EOF' > mapper.py
#!/usr/bin/env python3
import sys

n, p = 2, 2

for line in sys.stdin:
    parts = line.strip().split()
    if not parts:
        continue
    matrix, i, j, value = parts[0], int(parts[1]), int(parts[2]), int(parts[3])
    if matrix == 'A':
        for k in range(p):
            print(f"{i},{k}\tA,{j},{value}")
    else:
        for k in range(n):
            print(f"{k},{j}\tB,{i},{value}")
EOF

3. Create Reducer Script (reducer.py):
cat << 'EOF' > reducer.py
#!/usr/bin/env python3
import sys
from collections import defaultdict

current_key = None
a_values = defaultdict(int)
b_values = defaultdict(int)

def emit_result(key, a_vals, b_vals):
    total = 0
    for k in a_vals:
        if k in b_vals:
            total += a_vals[k] * b_vals[k]
    if total != 0:
        print(f"{key}\t{total}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value = line.split('\t')
    if current_key != key:
        if current_key is not None:
            emit_result(current_key, a_values, b_values)
        current_key = key
        a_values = defaultdict(int)
        b_values = defaultdict(int)
    matrix, k, val = value.split(',')
    k, val = int(k), int(val)
    if matrix == 'A':
        a_values[k] = val
    else:
        b_values[k] = val

if current_key:
    emit_result(current_key, a_values, b_values)
EOF

4. Execute MapReduce Job on HDFS:
chmod +x mapper.py reducer.py
hadoop fs -rm -r /result_matrix 2>/dev/null
hadoop fs -mkdir -p /matrix
hadoop fs -put matrix_a.txt /matrix/
hadoop fs -put matrix_b.txt /matrix/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /matrix/matrix_a.txt /matrix/matrix_b.txt \
  -output /result_matrix

5. Verify Output:
hadoop fs -cat /result_matrix/part-00000
"""

PROGRAM_3 = r"""================================================================================
PROGRAM 3: WEATHER DATA MINING (MAPREDUCE)
================================================================================

1. Create Input File:
cat << 'EOF' > weather.txt
date temperature condition
2023-01-01 28 Sunny
2023-01-02 22 Cloudy
2023-01-03 30 Sunny
2023-01-04 18 Rainy
2023-01-05 26 Cloudy
EOF

2. Create Mapper Script (weather_mapper.py):
cat << 'EOF' > weather_mapper.py
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split()
    
    if not parts or parts[0] == "date":
        continue
        
    date = parts[0]
    try:
        temperature = float(parts[1])
        condition = parts[2]
        print(f"{date}\t{temperature}\t{condition}")
    except (IndexError, ValueError):
        continue
EOF

3. Create Reducer Script (weather_reducer.py):
cat << 'EOF' > weather_reducer.py
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    parts = line.split("\t")
    if len(parts) < 3:
        continue
        
    date, temp, condition = parts[0], float(parts[1]), parts[2]
    
    if temp > 25:
        classification = "Hot Day"
    else:
        classification = "Cold Day"
        
    print(f"{date}\t{classification}")
EOF

4. Execute Job on HDFS:
chmod +x weather_mapper.py weather_reducer.py
hadoop fs -rm -r /weather_output 2>/dev/null
hadoop fs -mkdir -p /weather_input
hadoop fs -put weather.txt /weather_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files weather_mapper.py,weather_reducer.py \
  -mapper "python3 weather_mapper.py" \
  -reducer "python3 weather_reducer.py" \
  -input /weather_input/weather.txt \
  -output /weather_output

5. Verify Output:
hadoop fs -cat /weather_output/part-00000
"""

PROGRAM_4 = r"""================================================================================
PROGRAM 4: MOVIELENS TAG ASSOCIATION (MAPREDUCE)
================================================================================

1. Create Input Files:
cat << 'EOF' > movies.csv
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
2,Jumanji (1995),Adventure|Children|Fantasy
EOF

cat << 'EOF' > tags.csv
userId,movieId,tag,timestamp
15,1,funny,1215184630
20,2,Robin Williams,1215184631
15,1,Pixar,1215184633
EOF

2. Create Mapper Script (mapper.py):
cat << 'EOF' > mapper.py
#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
for row in reader:
    if len(row) >= 3:
        if row[0] in ("movieId", "userId"):
            continue
        movie_id = row[1].strip()
        tag = row[2].strip()
        if movie_id and tag:
            print(f"{movie_id}\t{tag}")
EOF

3. Create Reducer Script (reducer.py):
cat << 'EOF' > reducer.py
#!/usr/bin/env python3
import sys

current_movie = None
tags = []

for line in sys.stdin:
    line = line.strip()
    if not line or '\t' not in line:
        continue
    movie_id, tag = line.split('\t', 1)
    if current_movie == movie_id:
        tags.append(tag)
    else:
        if current_movie:
            print(f"{current_movie}\t{','.join(tags)}")
        current_movie = movie_id
        tags = [tag]

if current_movie:
    print(f"{current_movie}\t{','.join(tags)}")
EOF

4. Execute Job on HDFS:
chmod +x mapper.py reducer.py
hadoop fs -rm -r /output_movie 2>/dev/null
hadoop fs -mkdir -p /movielens_input
hadoop fs -put movies.csv /movielens_input/
hadoop fs -put tags.csv /movielens_input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /movielens_input/movies.csv /movielens_input/tags.csv \
  -output /output_movie

5. Verify Output:
hadoop fs -cat /output_movie/part-00000

--- Expected Output ---
1	funny,Pixar
2	Robin Williams
Jumanji (1995)	Adventure|Children|Fantasy
Toy Story (1995)	Adventure|Animation|Children|Comedy|Fantasy
"""

PROGRAM_5 = r"""================================================================================
PROGRAM 5: MONGODB OPERATIONS (COUNT, SORT, LIMIT, SKIP, AGGREGATE)
================================================================================

1. Start MongoDB Service & Launch Shell:
sudo systemctl start mongod
mongosh

2. Insert Employee Dataset:
db.employee.insertMany([
  {
    _id: 1,
    name: 'Kavana',
    age: 25,
    email: 'Kavana@example.com',
    city: 'Chicago',
    Department: "Finance",
    Gender: "Female",
    salary: 90000,
    JoiningDate: new ISODate("2020-05-18")
  },
  {
    _id: 2,
    name: 'Laksmi',
    age: 30,
    email: 'Laksmi@example.com',
    city: 'Chicago',
    Department: "HR Dept",
    Gender: "Female",
    salary: 80000,
    JoiningDate: new ISODate("2021-06-25")
  },
  {
    _id: 3,
    name: 'AMRUTHA',
    age: 35,
    email: 'AMRUTHA@example.com',
    city: 'Los Angeles',
    Department: "Development",
    Gender: "Female",
    salary: 70000,
    JoiningDate: new ISODate("2022-06-25")
  },
  {
    _id: 4,
    name: 'PANCHAMI C H',
    age: 35,
    email: 'PANCHAMI@example.com',
    city: 'Los Angeles',
    Department: "Development",
    Gender: "Female",
    salary: 70000,
    JoiningDate: new ISODate("2022-06-25")
  },
  {
    _id: 5,
    name: 'Rajesh',
    age: 25,
    email: 'Rajesh@example.com',
    city: 'Michigan',
    Department: "Finance",
    Gender: "Male",
    salary: 40000,
    JoiningDate: new ISODate("2015-11-25")
  },
  {
    _id: 6,
    name: 'Akash',
    age: 25,
    email: 'Akash@example.com',
    city: 'Michigan',
    Department: "Finance",
    Gender: "Male",
    salary: 40000,
    JoiningDate: new ISODate("2020-11-25")
  },
  {
    _id: 7,
    name: 'Nikitha',
    age: 25,
    email: 'Nikitha@example.com',
    city: 'Los Angeles',
    Department: "HR Dept",
    Gender: "Female",
    salary: 40000,
    JoiningDate: new ISODate("2018-04-25")
  },
  {
    _id: 8,
    name: 'MUZAMMIL HUSAIN ABOOBAKAR SHANTAGERI',
    age: 25,
    email: 'MUZAMMIL@example.com',
    city: 'Los Angeles',
    Department: "Development",
    Gender: "Male",
    salary: 55000,
    JoiningDate: new ISODate("2012-04-25")
  },
  {
    _id: 9,
    name: 'Ruthvi R Chawla',
    age: 25,
    email: 'Ruthvi@example.com',
    city: 'Chicago',
    Department: "Finance",
    Gender: "Female",
    salary: 90000,
    JoiningDate: new ISODate("2016-05-25")
  },
  {
    _id: 10,
    name: 'Ananya',
    age: 25,
    email: 'Ananya@example.com',
    city: 'Boston',
    Department: "HR Dept",
    Gender: "Female",
    salary: 40000,
    JoiningDate: new ISODate("2015-01-25")
  },
  {
    _id: 11,
    name: 'SHIVANI K K',
    age: 25,
    email: 'SHIVANIa@example.com',
    city: 'Los Angeles',
    Department: "Development",
    Gender: "Female",
    salary: 850000,
    JoiningDate: new ISODate("2013-01-25")
  },
  {
    _id: 12,
    name: 'VARSHA G R',
    age: 25,
    email: 'VARSHA@example.com',
    city: 'Boston',
    Department: "Development",
    Gender: "Female",
    salary: 850000,
    JoiningDate: new ISODate("2014-07-25")
  },
  {
    _id: 13,
    name: 'Shifa Banu',
    age: 25,
    email: 'Shifa@example.com',
    city: 'Michigan',
    Department: "Finance",
    Gender: "Female",
    salary: 990000,
    JoiningDate: new ISODate("2019-08-25")
  },
  {
    _id: 14,
    name: 'MOHAMMED ARSHAD',
    age: 25,
    email: 'ARSHAD@example.com',
    city: 'Michigan',
    Department: "Finance",
    Gender: "Male",
    salary: 750000,
    JoiningDate: new ISODate("2021-07-19")
  }
]);

--- Expected Insertion Output ---
{
  "acknowledged": true,
  "insertedIds": {
    "0": 1, "1": 2, "2": 3, "3": 4, "4": 5, "5": 6, "6": 7,
    "7": 8, "8": 9, "9": 10, "10": 11, "11": 12, "12": 13, "13": 14
  }
}

3. Query 1 - Count Total Employees (Aggregate & Count):
db.employee.aggregate([
  { $group: { _id: '', number_of_employees: { $sum: 1 } } }
])

db.employee.find().count()

--- Expected Output ---
[
  {
    "_id": "",
    "number_of_employees": 14
  }
]

4. Query 2 - Finance Employees Sorted by Salary Descending ($match, $project, $sort):
db.employee.aggregate([
  { $match: { Department: 'Finance' } },
  { $project: { "name": 1, age: 1, Department: 1, Gender: 1, salary: 1 } },
  { $sort: { salary: -1 } }
])

--- Expected Output ---
[
  { "_id": 13, "name": "Shifa Banu", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 990000 },
  { "_id": 14, "name": "MOHAMMED ARSHAD", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 750000 },
  { "_id": 1, "name": "Kavana", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 90000 },
  { "_id": 9, "name": "Ruthvi R Chawla", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 90000 },
  { "_id": 5, "name": "Rajesh", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 40000 },
  { "_id": 6, "name": "Akash", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 40000 }
]

5. Query 3 - Display First 2 Records ($limit):
db.employee.aggregate([
  { $limit: 2 }
])

--- Expected Output ---
[
  {
    "_id": 1,
    "name": "Kavana",
    "age": 25,
    "email": "Kavana@example.com",
    "city": "Chicago",
    "Department": "Finance",
    "Gender": "Female",
    "salary": 90000,
    "JoiningDate": { "$date": "2020-05-18T00:00:00Z" }
  },
  {
    "_id": 2,
    "name": "Laksmi",
    "age": 30,
    "email": "Laksmi@example.com",
    "city": "Chicago",
    "Department": "HR Dept",
    "Gender": "Female",
    "salary": 80000,
    "JoiningDate": { "$date": "2021-06-25T00:00:00Z" }
  }
]

6. Query 4 - Top 2 Highest-Paid Finance Employees ($match, $project, $sort, $limit):
db.employee.aggregate([
  { $match: { Department: 'Finance' } },
  { $project: { "name": 1, age: 1, Department: 1, Gender: 1, salary: 1 } },
  { $sort: { salary: -1 } },
  { $limit: 2 }
])

--- Expected Output ---
[
  { "_id": 13, "name": "Shifa Banu", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 990000 },
  { "_id": 14, "name": "MOHAMMED ARSHAD", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 750000 }
]

7. Query 5 - Skip First 6 Records ($skip):
db.employee.aggregate([
  { $skip: 6 }
])

--- Expected Output ---
[
  { "_id": 7, "name": "Nikitha", "age": 25, "email": "Nikitha@example.com", "city": "Los Angeles", "Department": "HR Dept", "Gender": "Female", "salary": 40000, "JoiningDate": { "$date": "2018-04-25T00:00:00Z" } },
  { "_id": 8, "name": "MUZAMMIL HUSAIN ABOOBAKAR SHANTAGERI", "age": 25, "email": "MUZAMMIL@example.com", "city": "Los Angeles", "Department": "Development", "Gender": "Male", "salary": 55000, "JoiningDate": { "$date": "2012-04-25T00:00:00Z" } },
  { "_id": 9, "name": "Ruthvi R Chawla", "age": 25, "email": "Ruthvi@example.com", "city": "Chicago", "Department": "Finance", "Gender": "Female", "salary": 90000, "JoiningDate": { "$date": "2016-05-25T00:00:00Z" } },
  { "_id": 10, "name": "Ananya", "age": 25, "email": "Ananya@example.com", "city": "Boston", "Department": "HR Dept", "Gender": "Female", "salary": 40000, "JoiningDate": { "$date": "2015-01-25T00:00:00Z" } },
  { "_id": 11, "name": "SHIVANI K K", "age": 25, "email": "SHIVANIa@example.com", "city": "Los Angeles", "Department": "Development", "Gender": "Female", "salary": 850000, "JoiningDate": { "$date": "2013-01-25T00:00:00Z" } },
  { "_id": 12, "name": "VARSHA G R", "age": 25, "email": "VARSHA@example.com", "city": "Boston", "Department": "Development", "Gender": "Female", "salary": 850000, "JoiningDate": { "$date": "2014-07-25T00:00:00Z" } },
  { "_id": 13, "name": "Shifa Banu", "age": 25, "email": "Shifa@example.com", "city": "Michigan", "Department": "Finance", "Gender": "Female", "salary": 990000, "JoiningDate": { "$date": "2019-08-25T00:00:00Z" } },
  { "_id": 14, "name": "MOHAMMED ARSHAD", "age": 25, "email": "ARSHAD@example.com", "city": "Michigan", "Department": "Finance", "Gender": "Male", "salary": 750000, "JoiningDate": { "$date": "2021-07-19T00:00:00Z" } }
]

8. Query 6 - Finance Employees Skipping Top 2 Salaries ($match, $project, $sort, $skip):
db.employee.aggregate([
  { $match: { Department: 'Finance' } },
  { $project: { "name": 1, age: 1, Department: 1, Gender: 1, salary: 1 } },
  { $sort: { salary: -1 } },
  { $skip: 2 }
])

--- Expected Output ---
[
  { "_id": 1, "name": "Kavana", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 90000 },
  { "_id": 9, "name": "Ruthvi R Chawla", "age": 25, "Department": "Finance", "Gender": "Female", "salary": 90000 },
  { "_id": 5, "name": "Rajesh", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 40000 },
  { "_id": 6, "name": "Akash", "age": 25, "Department": "Finance", "Gender": "Male", "salary": 40000 }
]
"""

PROGRAM_6 = r"""================================================================================
PROGRAM 6: PIG LATIN OPERATIONS (SORT, GROUP, JOIN, PROJECT, FILTER)
================================================================================

1. Create Input Files:
cat << 'EOF' > employees.txt
101,John,30,1,50000
102,Sam,28,2,60000
103,Anna,32,1,75000
104,David,29,3,62000
105,Lily,27,2,58000
EOF

cat << 'EOF' > departments.txt
1,HR
2,Finance
3,IT
EOF

2. Create Pig Latin Script (employee_analysis.pig):
cat << 'EOF' > employee_analysis.pig
employees = LOAD 'hdfs://localhost:9000/user/cloudera/employees.txt' USING PigStorage(',') AS (emp_id:int, name:chararray, age:int, dept_id:int, salary:int);
departments = LOAD 'hdfs://localhost:9000/user/cloudera/departments.txt' USING PigStorage(',') AS (dept_id:int, dept_name:chararray);

filtered_employees = FILTER employees BY age > 28;
projected_employees = FOREACH filtered_employees GENERATE emp_id, name, salary;
sorted_employees = ORDER projected_employees BY salary DESC;
grouped_by_department = GROUP employees BY dept_id;
joined_data = JOIN employees BY dept_id, departments BY dept_id;

STORE sorted_employees INTO 'hdfs://localhost:9000/user/cloudera/output/sorted_employees' USING PigStorage(',');
STORE grouped_by_department INTO 'hdfs://localhost:9000/user/cloudera/output/grouped_by_department' USING PigStorage(',');
STORE joined_data INTO 'hdfs://localhost:9000/user/cloudera/output/joined_data' USING PigStorage(',');

DUMP sorted_employees;
DUMP grouped_by_department;
DUMP joined_data;
EOF

3. Upload Data & Execute Script on HDFS:
hdfs dfs -rm -r /user/cloudera/output 2>/dev/null
hdfs dfs -mkdir -p /user/cloudera
hdfs dfs -put -f employees.txt /user/cloudera/
hdfs dfs -put -f departments.txt /user/cloudera/

pig -x mapreduce employee_analysis.pig

4. Verify Output:
hdfs dfs -cat /user/cloudera/output/sorted_employees/part-r-00000
hdfs dfs -cat /user/cloudera/output/grouped_by_department/part-r-00000
hdfs dfs -cat /user/cloudera/output/joined_data/part-r-00000

--- Expected Output (Sorted Employees by Salary) ---
103,Anna,75000
104,David,62000
102,Sam,60000
101,John,50000
"""

PROGRAM_7 = r"""================================================================================
PROGRAM 7: HIVE OPERATIONS (DATABASES, TABLES, VIEWS, FUNCTIONS, INDEXES)
================================================================================

0. Upload Dataset to HDFS & Launch Hive Shell:
# (If employees.txt is not yet created, create it first)
cat << 'EOF' > employees.txt
101,John,30,1,50000
102,Sam,28,2,60000
103,Anna,32,1,75000
104,David,29,3,62000
105,Lily,27,2,58000
EOF

hdfs dfs -mkdir -p /user/cloudera
hdfs dfs -put -f employees.txt /user/cloudera/
hive

1. Create a Database:
CREATE DATABASE employee_db;

--- Expected Output ---
OK

2. Use the Database:
USE employee_db;

--- Expected Output ---
OK

3. Alter a Database:
ALTER DATABASE employee_db SET DBPROPERTIES ('Owner'='Admin');

--- Expected Output ---
OK

4. Create a Table:
CREATE TABLE employees (emp_id INT, name STRING, age INT, dept_id INT, salary FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

--- Expected Output ---
OK

5. Load Data into Table:
LOAD DATA INPATH '/user/cloudera/employees.txt' INTO TABLE employees;

--- Expected Output ---
Loading data to table employee_db.employees
OK

6. Alter a Table (Add a New Column):
ALTER TABLE employees ADD COLUMNS (email STRING);

--- Expected Output ---
OK

7. Rename Table (and Restore Name for Next Queries):
ALTER TABLE employees RENAME TO employees_new;
ALTER TABLE employees_new RENAME TO employees;

--- Expected Output ---
OK
OK

8. Create a View:
CREATE VIEW high_salary_employees AS SELECT emp_id, name, salary FROM employees WHERE salary > 50000;

--- Expected Output ---
OK

9. Alter a View:
ALTER VIEW high_salary_employees AS SELECT emp_id, name, age, salary FROM employees WHERE salary > 60000;

--- Expected Output ---
OK

10. Create a Function (Register Java UDF):
ADD JAR /user/cloudera/custom_udf.jar;
CREATE FUNCTION to_upper AS 'com.example.hiveudf.ToUpperUDF';

--- Expected Output ---
Added /user/cloudera/custom_udf.jar to class path
OK

11. Use the Function:
SELECT to_upper(name) FROM employees;

--- Expected Output ---
JOHN
SAM
ANNA
DAVID
LILY
OK

12. Create an Index:
CREATE INDEX emp_dept_idx ON TABLE employees (dept_id) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;

--- Expected Output ---
OK

13. Rebuild the Index:
ALTER INDEX emp_dept_idx ON employees REBUILD;

--- Expected Output ---
OK

14. Check All Databases:
SHOW DATABASES;

--- Expected Output ---
default
employee_db
OK

15. Check All Tables in the Current Database:
SHOW TABLES;

--- Expected Output ---
employees
high_salary_employees
OK

16. Describe Table Schema:
DESCRIBE employees;

--- Expected Output ---
emp_id              	int                 	                    
name                	string              	                    
age                 	int                 	                    
dept_id             	int                 	                    
salary              	float               	                    
email               	string              	                    
OK

17. Display Table Data:
SELECT * FROM employees LIMIT 5;

--- Expected Output ---
101	John	30	1	50000.0	NULL
102	Sam	28	2	60000.0	NULL
103	Anna	32	1	75000.0	NULL
104	David	29	3	62000.0	NULL
105	Lily	27	2	58000.0	NULL
OK
"""

PROGRAM_8 = r"""================================================================================
PROGRAM 8: WORD COUNT PROGRAM (HADOOP MAPREDUCE)
================================================================================

1. Create Input File:
cat << 'EOF' > input.txt
mysuru
bangalore
hubli
mysuru
bangalore
mangalore
bangalore
EOF

2. Create Mapper Script (mapper.py):
cat << 'EOF' > mapper.py
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print('{}\t{}'.format(word, 1))
EOF

3. Create Reducer Script (reducer.py):
cat << 'EOF' > reducer.py
#!/usr/bin/env python3
from collections import defaultdict
import sys

word_counts = defaultdict(int)

for line in sys.stdin:
    try:
        line = line.strip()
        word, count = line.split()
        count = int(count)
    except:
        continue
    word_counts[word] += count

for word, count in word_counts.items():
    print(word, count)
EOF

4. Execute Job on HDFS:
chmod +x mapper.py reducer.py
hadoop fs -rm -r /word_count.txt 2>/dev/null
hadoop fs -put -f input.txt /

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /input.txt \
  -output /word_count.txt

5. Verify Output:
hadoop fs -cat /word_count.txt/part-00000

--- Expected Output ---
bangalore 3
hubli 1
mangalore 1
mysuru 2
"""

PROGRAM_TITLES = [
    (1, "HDFS File Management Tasks"),
    (2, "MapReduce Matrix Multiplication"),
    (3, "Weather Data Mining (MapReduce)"),
    (4, "MovieLens Tag Association (MapReduce)"),
    (5, "MongoDB Operations (Count, Sort, Limit, Skip, Aggregate)"),
    (6, "Pig Latin Operations (Sort, Group, Join, Project, Filter)"),
    (7, "Hive Operations (Databases, Tables, Views, Functions, Indexes)"),
    (8, "Word Count Program (Hadoop MapReduce)"),
]

PROGRAM_MAP = {
    1: PROGRAM_1,
    2: PROGRAM_2,
    3: PROGRAM_3,
    4: PROGRAM_4,
    5: PROGRAM_5,
    6: PROGRAM_6,
    7: PROGRAM_7,
    8: PROGRAM_8,
}


def p1():
    """Program 1: HDFS File Management Tasks"""
    print(PROGRAM_1)


def p2():
    """Program 2: MapReduce Matrix Multiplication"""
    print(PROGRAM_2)


def p3():
    """Program 3: Weather Data Mining (MapReduce)"""
    print(PROGRAM_3)


def p4():
    """Program 4: MovieLens Tag Association (MapReduce)"""
    print(PROGRAM_4)


def p5():
    """Program 5: MongoDB Operations (Count, Sort, Limit, Skip, Aggregate)"""
    print(PROGRAM_5)


def p6():
    """Program 6: Pig Latin Operations (Sort, Group, Join, Project, Filter)"""
    print(PROGRAM_6)


def p7():
    """Program 7: Hive Operations (Databases, Tables, Views, Functions, Indexes)"""
    print(PROGRAM_7)


def p8():
    """Program 8: Word Count Program (Hadoop MapReduce)"""
    print(PROGRAM_8)


def list_all():
    """Print the titles and function references for all 8 programs."""
    header = "=" * 80
    print(header)
    print("BIG DATA LABORATORY MANUAL - PROGRAM DIRECTORY")
    print(header)
    for num, title in PROGRAM_TITLES:
        print(f"  lab.p{num}() -> Program {num}: {title}")
    print(header)
    print("Usage: Call lab.p1() through lab.p8() to display instructions and code.")


def main():
    """Command-line interface entry point."""
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help", "list", "all"):
        list_all()
        return

    arg = args[0].lower().replace("p", "")
    try:
        num = int(arg)
        if num in PROGRAM_MAP:
            print(PROGRAM_MAP[num])
        else:
            print(f"Invalid program number: {args[0]}. Valid options are 1 through 8 (e.g. 'p1').")
            sys.exit(1)
    except ValueError:
        print(f"Unknown argument: {args[0]}. Run without arguments or with 'list' to see all programs.")
        sys.exit(1)


if __name__ == "__main__":
    main()
