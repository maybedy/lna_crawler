from threading import Thread
import math

from . import Search
from .db import get_collection
from tqdm import tqdm

result_name = 'results'
error_list_name = 'errors'

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
        self.keywords = keywords['keywords']
        self.id = keywords['id']
        self.period = period
        self.result = result

    def to_json(self):
        return {
            'keywords_id': self.id,
            'keywords': self.keywords,
            'from_year': self.period.from_year,
            'from_month': self.period.from_month,
            'to_year': self.period.to_year,
            'to_month': self.period.to_month,
            'result': self.result
        }

class Run(object):
    def __init__(self, input_file_name, input_path, output_path, from_year, to_year):
        self.db_name = input_file_name
        self.input_path = input_path
        self.output_path = output_path
        self.keywords_list = []
        self.from_year = from_year
        self.to_year = to_year
        self.search = Search()
        self._load_input()

    def _load_input(self):
        with open(self.input_path, 'r') as file:
            while True:
                line = file.readline()
                if line:
                    words = line.split(",")
                    keywords = []
                    keywords_id = words[0]
                    if keywords_id == "":
                        continue
                    for i in range(1, len(words)):
                        word = words[i].strip()
                        if word == "" or len(word) == 0 or word == '':
                            pass
                        else:
                            keywords.append(word)
                    keywords = sorted([word.strip() for word in keywords])
                    self.keywords_list.append({'id': keywords_id, 'keywords':keywords})
                else:
                    break

    def _search(self, keywords, period:Period):
        try:
            result = self.search.search(keywords['keywords'], period.get_str_from(), period.get_str_to())
            print("Search Success : {}-{} = {},{}".format(period.get_str_from(), period.get_str_to(), str(result),
                                                          keywords['id']))
            return result
        except:
            print("Error ! : {}-{} = {}".format(period.get_str_from(), period.get_str_to(), keywords['id']))
            get_collection(db_name=self.db_name, collection_name=error_list_name).insert_one(
                {"id": keywords['id'],
                 "keywords": keywords['keywords'],
                 "from_year": period.from_year,
                 "from_month":period.from_month,
                 "to_year": period.to_year,
                 "to_month": period.to_month})
            return None

    def _periodic_search(self, keywords, period_list:list):
        for period in period_list:
            result = self._search(keywords, period)
            if result is not None:
                result = Result(keywords, period, result)
                get_collection(db_name= self.db_name, collection_name=result_name).insert_one(result.to_json())
            else:
                pass

    def _yearly_search(self, keywords, year:int):
        period = Period(year, 1, year, 12)
        result = self._search(keywords, period)
        if result is not None:
            if result < 3000:
                result = Result(keywords, period, result)
                get_collection(db_name=self.db_name, collection_name=result_name).insert_one(result.to_json())
            else:
                period_list = []
                for i in range(6):
                    period_list.append(Period(year, i * 2 + 1, year, i * 2 + 2))
                self._periodic_search(keywords, period_list)

    def _get_result(self, keywords_list):
        print("Search start ! ")
        pbar = tqdm(total=(self.to_year - self.from_year + 1) * len(keywords_list))
        for keywords in keywords_list:
            for year in range(self.from_year, self.to_year+1):
                self._yearly_search(keywords, year)
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
        print("Make Report !")
        pbar = tqdm(total = len(self.keywords_list) * (self.to_year - self.from_year + 1))
        result_collection = get_collection(db_name=self.db_name, collection_name=result_name)
        for keywords in self.keywords_list:
            keywords_id = keywords["id"]
            for year in range(self.from_year, self.to_year+1):
                result_count = result_collection.count({"keywords_id": keywords_id, "from_year": year})
                if result_count == 0:
                    print("lack of result - search again : {}-{}".format(year, keywords_id))
                    self._yearly_search(keywords, year)
                count = 0
                while True:
                    count = 0
                    finished = True
                    year_count_one = False
                    yearly_results = result_collection.find({"keywords_id": keywords_id, "from_year": year})
                    already_checked = []
                    for result in yearly_results:
                        from_month = result['from_month']
                        to_month = result['to_month']

                        if from_month in already_checked:
                            print("double result - {}.{} : {}".format(year, from_month, keywords_id))
                            continue
                        if from_month == 1 and to_month == 12:
                            year_count_one = True
                            count = result["result"]
                            break
                        else:
                            count += result["result"]
                            already_checked.append(from_month)
                    if not year_count_one:
                        period_list = []
                        for i in range(6):
                            month = i * 2 + 1
                            if month not in already_checked:
                                print("Unfinished period occur")
                                finished = False
                                period = Period(year, month, year, month+1)
                                period_list.append(period)
                        if not finished:
                            self._periodic_search(keywords, period_list)
                        else:
                            break
                    else:
                        break
                file.write(",".join([keywords_id, str(year), str(count)]) + "\n")
                file.flush()
                pbar.update(1)

    def _resolve_errors(self):
        while True:
            collection = get_collection(db_name=self.db_name, collection_name=error_list_name)
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
                    collection.delete_one(error)
                    count += 1
                    from_year = error['from_year']
                    from_month = error['from_month']
                    to_year = error['to_year']
                    to_month = error['to_month']
                    keywords = {'id': error['id'], 'keywords': error['keywords']}
                    if from_month == 1 and to_month == 12:
                        self._yearly_search(keywords, from_year)
                    else:
                        period = Period(from_year, from_month, to_year, to_month)
                        self._periodic_search(keywords, [period])
                    pbar.update(1)
            else:
                break


    def run(self, thread_count=1):
        thread_list = []
        # keywords_list = self._divide_keywords_list(thread_count)

        print("Crawling start!")
        #self._get_result(keywords_list[0])
        self._resolve_errors()
        self._make_report()
