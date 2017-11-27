from threading import Thread
import math

from . import Search
from .db import get_collection
from tqdm import tqdm

class Period(object):
    def __init__(self, from_year, from_month, to_year, to_month):
        self.from_year = from_year
        self.from_month = from_month
        self.to_year = to_year
        self.to_month = to_month

    def to_json(self):
        return {'from_year': self.from_year,
                'from_month': self.from_month,
                'to_year': self.to_year,
                'to_month': self.to_month}

    def get_str_from(self):
        return "{}.{}".format(self.from_year, self.from_month)

    def get_str_to(self):
        return "{}.{}".format(self.to_year, self.to_month)

class Result(object):
    def __init__(self, keywords, period:Period, result):
        self.keywords = keywords
        self.period = period
        self.result = result

    def to_json(self):
        return {
            'keywords_id': "|".join(sorted(self.keywords)),
            'keywords': self.keywords,
            'from_year': self.period.from_year,
            'from_month': self.period.from_month,
            'to_year': self.period.to_year,
            'to_month': self.period.to_month,
            'result': self.result
        }

class Run(object):
    def __init__(self, input_path, output_path, from_date, to_date):
        self.input_path = input_path
        self.output_path = output_path
        self.keywords_list = []
        self.from_date = from_date
        self.to_date = to_date

        self.period_list = []
        for year in range(from_date, to_date+1):
            for i in range(6):
                month = i * 2 + 1
                period = Period(year, month, year, month+1)
                self.period_list.append(period)

        self._load_input()

    def _load_input(self):
        with open(self.input_path, 'r') as file:
            while True:
                line = file.readline()
                if line:
                    keywords = line.split(",")
                    keywords = sorted([word.strip() for word in keywords])
                    self.keywords_list.append(keywords)
                else:
                    break

    def _get_result(self, keywords_list):
        search = Search()
        print("Search start ! ")
        pbar = tqdm(total=len(self.period_list) * len(keywords_list))
        for keywords in keywords_list:
            for period in self.period_list:
                try:
                    collection = get_collection(collection_name="results")
                    result = search.search(keywords, period.get_str_from(), period.get_str_to())
                    result = Result(keywords, period, result)
                    collection.insert_one(result.to_json())
                    print("Search Success : {}-{} = {},{}".format(period.get_str_from(), period.get_str_to(), str(result),"|".join(keywords)))
                except:
                    print("Error ! : {}-{} = {}".format(period.get_str_from(), period.get_str_to(), "|".join(keywords)))
                    collection = get_collection(collection_name="error_list")
                    collection.insert_one({"keywords": keywords, "from_date": period.get_str_from(), "to_date": period.get_str_to()})
                pbar.update(1)

    def _divide_keywords_list(self, thread_count):
        keywords_list = []
        max_keywords_len = int(math.ceil(len(self.keywords_list) / thread_count))
        count = 0
        for i in range(max_keywords_len):
            for j in range(thread_count):
                if len(keywords_list) <= j:
                    keywords_list.append([])
                if count < len(self.keywords_list):
                    keywords_list[j].append(self.keywords_list[count])
                    # TODO Send self.keywords_list[count] to DB
                    count += 1
                else:
                    break

        return keywords_list

    def _make_report(self):
        file = open(self.output_path, 'w')
        collection = get_collection(collection_name="results")
        print("Make Report !")
        pbar = tqdm(total = len(self.keywords_list) * (self.to_date - self.from_date + 1) * 6)
        for keywords in self.keywords_list:
            keywords_id = "|".join(sorted(keywords))
            for year in range(self.from_date, self.to_date+1):
                yearly_results = collection.find({"keywords_id": keywords_id, "from_year": year})
                count = 0
                for result in yearly_results:
                    count += result["result"]
                    pbar.update(1)
                file.write(",".join([keywords_id, str(year), str(count)]))

    def _resolve_errors(self):
        search = Search()
        while True:
            collection = get_collection(collection_name="error_list")

            error_count = collection.count()
            if error_count > 0:
                print("Last Error Count : {} - RETRY!!!".format(error_count))

                errors = collection.find()
                count = 0
                pbar = tqdm(total=error_count)
                while True:
                    if count >= error_count:
                        break
                    error = next(errors)

                    collection = get_collection(collection_name="error_list")
                    collection.delete_one(error)
                    count += 1
                    try:
                        collection = get_collection(collection_name="results")
                        result = search.search(error['keywords'], error['from_date'], error['to_date'])
                        from_year = int(error['from_date'].split(".")[0])
                        from_month = int(error['from_date'].split(".")[1])
                        to_year = int(error['to_date'].split(".")[0])
                        to_month = int(error['to_date'].split(".")[1])
                        period = Period(from_year, from_month, to_year, to_month)
                        result = Result(error['keywords'], period, result)
                        collection.insert_one(result.to_json())
                    except:
                        collection = get_collection(collection_name="error_list")
                        collection.insert_one(error)
                    pbar.update(1)
            else:
                break


    def run(self, thread_count=1):
        thread_list = []
        keywords_list = self._divide_keywords_list(thread_count)

        print("Crawling start!")
        self._get_result(keywords_list[0])
        #
        # for i in range(thread_count):
        #     t = Thread(target=self._get_result, kwargs={'keywords_list': keywords_list[i]}, name="T"+str(i))
        #     t.start()
        #     thread_list.append(t)
        #
        # for i in range(thread_count):
        #     thread_list[i].join()

        self._resolve_errors()
        self._make_report()
