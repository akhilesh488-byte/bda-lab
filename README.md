# BDA Lab (Big Data Laboratory)

Pip-installable reference library containing complete terminal-ready instructions, MapReduce scripts, HDFS commands, MongoDB aggregation queries, Pig Latin scripts, and HiveQL operations for the Big Data Analytics Laboratory.

All `gedit` GUI commands have been converted into clean `cat << 'EOF' > <filename>` heredoc blocks so code and input files can be pasted directly into an Ubuntu / Cloudera terminal or SSH session.

---

## 🚀 Quick Install on Ubuntu / Linux


### Option 1: Install directly via Git
```bash
pip install git+https://github.com/akhilesh488-byte/bda-lab.git
```

### Option 2: Install via Zip Archive (no Git required)
```bash
pip install https://github.com/akhilesh488-byte/bda-lab/archive/refs/heads/main.zip
```

### Option 3: Local Clone / Install
```bash
git clone https://github.com/akhilesh488-byte/bda-lab.git
cd bda-lab
pip install .
```

---

## 💻 Usage

### In Python Terminal / One-Liner
```bash
# View list of all 8 programs
python3 -c "import lab; lab.list_all()"

# Print Program 1 (HDFS File Management Tasks)
python3 -c "import lab; lab.p1()"

# Print Program 2 (MapReduce Matrix Multiplication)
python3 -c "import lab; lab.p2()"

# Print Program 8 (Word Count Program)
python3 -c "import lab; lab.p8()"
```

### In Python Interactive Shell (`python3`)
```python
import lab

# List all programs
lab.list_all()

# Print specific program instructions & code
lab.p1()  # HDFS File Management
lab.p2()  # MapReduce Matrix Multiplication
lab.p3()  # Weather Data Mining
lab.p4()  # MovieLens Tag Association
lab.p5()  # MongoDB Operations
lab.p6()  # Pig Latin Operations
lab.p7()  # Hive Operations
lab.p8()  # Word Count Program
```

### Direct CLI Command (Installed Console Script)
```bash
bda-lab         # Show program directory
bda-lab p1      # Output Program 1
bda-lab p2      # Output Program 2
# ...
bda-lab p8      # Output Program 8
```
or with Python module execution:
```bash
python3 -m lab p1
```

---

## 📚 Programs Included

| Function | Title | Description |
| :--- | :--- | :--- |
| `lab.p1()` | **HDFS File Management Tasks** | Daemon startup, directory creation, put, get, cat, and rm operations. |
| `lab.p2()` | **MapReduce Matrix Multiplication** | Python MapReduce streaming job for $2 \times 2$ matrix multiplication. |
| `lab.p3()` | **Weather Data Mining** | MapReduce streaming job classifying daily temperatures (Hot vs Cold Day). |
| `lab.p4()` | **MovieLens Tag Association** | MapReduce streaming join/association of movies and tag data. |
| `lab.p5()` | **MongoDB Operations** | Aggregation pipeline: `$match`, `$project`, `$sort`, `$limit`, `$skip`, `$group`, and `count`. |
| `lab.p6()` | **Pig Latin Operations** | Pig scripts using `FILTER`, `FOREACH GENERATE`, `ORDER BY`, `GROUP BY`, and `JOIN`. |
| `lab.p7()` | **Hive Operations** | Databases, tables, schema alterations, views, custom UDFs, indexes, and queries. |
| `lab.p8()` | **Word Count Program** | Standard Hadoop MapReduce Python streaming word count implementation. |

---

## 📁 Repository Structure

```
bda-lab/
├── lab.py              # Core Python module with programs 1-8 & list_all()
├── setup.py            # Package setup configuration for pip
├── README.md           # Documentation & quickstart guide
└── docs/
    └── gg2.txt         # Original lab manual reference file
```
