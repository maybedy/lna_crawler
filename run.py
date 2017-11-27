from lna_crawler.run import Run

input_path = './input.csv'
output_path = './output.csv'
from_date = 2001
to_date = 2001


# Recommended : use only 1 thread(lna recognized it as attack)

if __name__ == "__main__":
    runner = Run(input_path=input_path,
                 output_path=output_path,
                 from_date=from_date,
                 to_date=to_date)
    runner.run(thread_count=1)