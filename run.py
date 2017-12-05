from lna_crawler.run import Run

input_path_list = ['./list_1_REAL.csv']
output_path = './output.csv'
from_date = 2001
to_date = 2016


# Recommended : use only 1 thread(lna recognized it as attack)

if __name__ == "__main__":
    for input_path in input_path_list:
        input_path_file_name = input_path.split("/")[-1].split(".")
        input_path_file_name.remove(input_path_file_name[-1])
        output_path = ".".join(input_path_file_name) + "_output.csv"
        runner = Run(input_path=input_path,
                     output_path=output_path,
                     from_year=from_date,
                     to_year=to_date,
                     input_file_name=".".join(input_path_file_name))
        runner.run(thread_count=1)