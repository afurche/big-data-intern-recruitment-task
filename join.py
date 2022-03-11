import dask.dataframe as dd
import sys
import csv
import os


def join_csv():
    try:
        df1 = dd.read_csv(f'{sys.argv[2]}.csv')
        df2 = dd.read_csv(f'{sys.argv[3]}.csv')
    except FileNotFoundError as err:
        print(f"File not found: {err.filename}")
        return

    try:  # check if user typed join type
        join_type = sys.argv[5].lower()  # lower in case user types join type with some/all uppercase letters
        if join_type not in {'inner', 'outer', 'left', 'right'}:
            print(f"Invalid join type: '{join_type}'")
            return
    except IndexError:
        join_type = 'inner'  # inner join on default if not stated in command

    try:  # check if user typed correct column name and join type
        result = df1.merge(df2, on=sys.argv[4], how=join_type)
    except KeyError as err:
        print(f"Invalid column name: {err}")
        return

    # based on the user choice the result can be either:
    # 'to_csv' : written to csv file
    # 'print_head' : head printed in standard output
    # 'print_head_csv' : head printed in standard output and whole result written to standard output
    # 'print_all_memory' : whole result written in standard output (should be only used when the result can be stored in memory !)
    # 'print_all_csv' : written to csv file and whole content of the result printed (from csv file)
    result_type = sys.argv[1].lower()

    if result_type in {'to_csv', 'print_head', 'print_head_csv', 'print_all_memory', 'print_all_csv'}:

        if result_type == 'to_csv':
            result.to_csv(f'join_{os.path.basename(sys.argv[2])}_{os.path.basename(sys.argv[3])}_on_{sys.argv[4]}_{join_type}.csv', single_file=True)
        elif result_type == 'print_head':
            print(result.head())
        elif result_type == 'print_head_csv':
            print(result.head())
            result.to_csv(f'join_{os.path.basename(sys.argv[2])}_{os.path.basename(sys.argv[3])}_on_{sys.argv[4]}_{join_type}.csv', single_file=True)
        elif result_type == 'print_all_memory':
            print(result.compute())
        elif result_type == 'print_all_csv':
            result.to_csv(f'join_{os.path.basename(sys.argv[2])}_{os.path.basename(sys.argv[3])}_on_{sys.argv[4]}_{join_type}.csv', single_file=True)
            with open(f'join_{os.path.basename(sys.argv[2])}_{os.path.basename(sys.argv[3])}_on_{sys.argv[4]}_{join_type}.csv') as result:
                reader = csv.reader(result)
                for row in reader:
                    print(' '.join([str(item) for item in row]))
    else:
        print(f'Invalid result type: {result_type}')
        return


if __name__ == '__main__':
    join_csv()
