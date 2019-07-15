Programming Assignment for the class COSI127B - Database System Management at Brandeis University.

Leaderboard: https://pa3.rmarcus.info/
Nickname: Jvcki Wai

Given data and a specific subset of possible SQL queries, this program will load in the data and execute the SQL queries.

The Loader will read in the CSV data and parse the integers, saving them as 32-bit integers in a binary file. While the Loader is loading the data,
metadata (min, max, number of unique values for each column) will be taken and stored in what is called a Catalog (just holds the metadata).
The program will then take in a specific subset of possible SQL queries and the Parser will parse the queries, putting the information into a
format that the Optimizer can understand. The Optimizer will see what query needs to be run and uses Selinger's Algorithm, along with the metadata
in the Catalog, to make an estimate for the optimal join order. The Optimizer also passes the query, in a particular join order, to the Execution Engine,
which will take in that information and run the query.

This assignment was not just about getting the correct answer, but more about how quickly the program can get the correct answer, in other words,
how optimized the code was.

The important lesson to take away is that reading from disk is very slow compared to other operations, so the speed the program can calculate
the queries depends largely on minimizing the amount of reads to disk, and minimizing the time it takes to read files on disk.

Note: Actual code is in dist. Also, the data files were just too big to fit on Github. They can be found (as a download link) in dist/PA3.pdf.
