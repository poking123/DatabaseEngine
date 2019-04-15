import glob
import os.path
import subprocess
import sys
import time
from timeit import default_timer as timer

tables = []
for csv in glob.glob(os.path.join(sys.argv[1], "*.csv")):
    tables.append(csv)
print(tables)

# makes the 2 files
# proc_stdout and
# proc_stderr
stdout_f = open("proc_stdout", "w")
stderr_f = open("proc_stderr", "w")

# calls shell script to run your file
proc = subprocess.Popen(
    ["C:/Program Files (x86)/Java/jdk-11.0.2/bin/java", "DatabaseEngine"],
    encoding="utf-8",
    stdin=subprocess.PIPE
)

proc.stdin.write(",".join(tables) + "\n")
proc.stdin.flush()

time.sleep(1)

with open(os.path.join(sys.argv[1], "queries.sql")) as f:
    sql = f.read()

num_queries = sql.count(";")
proc.stdin.write(f"{num_queries}\n")
proc.stdin.write(sql)
proc.stdin.flush()


start = timer()
proc.wait(timeout=60*10 + 60*5)
stop = timer()

print("Total time:", (stop - start))
