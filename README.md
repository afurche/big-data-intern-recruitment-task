# big-data-intern-recruitment-task
 Recruitment task for big data internship at VirtusLab

My technology of choice to create this script was Python, simply because it is the technology I'm most comfortable in. To handle loading and joining of csv files I've decided to use Dask library, which provides Data Frames which work in a simmilar fashion to one found in Pandas, altough allow to handle datasets which size is bigger than available memory (which was part of the task assumptions).

Dask Data Frames do not store the data in memory (like their equivalents in Pandas do), but only store task graphs which are to be executed in lazy way when required. 

Considering join operation, there's also no risk of running out of memory, but computing joins on very large unsorted files is very demanding and can be really slow. 

The most challenging decision to make was how to handle the output of the script. If we assume that each input file can be much larger than memory, hence we also need to assume that output (the result of join) can also be too large to fit in. In dask the only way to receive results which can be directly printed to standard output is to get a Pandas Data Frame, which wouldn't work for very large results.

How to run the script:
```
    join *result_type* *file_path1* *file_path2* *column_name* [*join_type*]
```

Due to upper mentioned challenge I've decided to give user the choice of 5 possible ways to receive the output:
- `to_csv` : the entire result is saved to a new csv file and nothing gets printed in the standard output
- `print_head` : prints only the first 5 rows of the result to the standard output without saving it to csv file
- `print_head_csv` : prints the first 5 rows of the results and writes whole result to a new csv file
- `print_all_memory` : prints the whole results 'from memory', **shouldn't be used if user expects the results to be  bigger than memory !**
- `print_all_csv` : the results is saved to a csv file, then using a csv reader it is printed line by line to the standard output without the risk of running out of memory

In the `file_path` arguments .csv extension is **not** required.

`join_type` is optional (on default: 'inner')

Possible invalid user inputs are handled. 

Tests conducted:

Script written and tested in Python 3.9 and on Windows 10

All four types of joins performed on small test files (test files and results provided in repository)

I didn't test the script on very large files (ones that exceed the machine's memory), because considering the nature of Dask framework (and csv reader) there is no possibility of exceeding the memory.


