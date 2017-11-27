import time
from selenium import webdriver
from configparser import ConfigParser

from .definition import *


class Search(object):
    def __init__(self, b_pbar=True):
        config_parser = ConfigParser()
        config_parser.read("./crawler_config.ini")
        self.b_pbar = b_pbar
        self.url = config_parser["crawler"]["url"]
        self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(10)

    def search(self, keywords, from_date, to_date):
        self._init_page()
        self._change_condition(from_date, to_date)
        self._put_keywords(keywords)
        self._click_search()
        time.sleep(2)
        result = self._parse_result()
        return result

    def _init_page(self):
        self.driver.get(self.url)
        frame = self.driver.find_element_by_id('mainFrame')
        self.driver.switch_to.frame(frame)
        self.driver.find_element_by_id('news').click()
        time.sleep(1)
        newspaper = self.driver.find_element_by_css_selector("a[href='form_news_wires.asp']")
        newspaper.click()

    def _put_keywords(self, keywords):
        assert len(keywords) <= 5
        for i in range(len(keywords)):
            if i == 0:
                terms = self.driver.find_element_by_id(KEYWORD_ID)
            else:
                try:
                    terms = self.driver.find_element_by_id(KEYWORD_ID + str(i + 1))
                    search_connector = self.driver.find_element_by_name(CONNECTOR_NAME + str(i + 1))
                    if i > 2: # TODO
                        self.driver.find_element_by_id("addRowLink").click()
                    search_connector.send_keys("OR")
                except:
                    pass
            label = self.driver.find_element_by_name(LABEL_NAME + str(i + 1))
            terms.clear()
            label.send_keys(LABEL_VALUE)
            terms.send_keys(keywords[i])


    def _change_condition(self, from_date, to_date):
        uncheck = self.driver.find_element_by_id('8411')
        check = self.driver.find_element_by_id('140954')
        if uncheck.is_selected():
            uncheck.click()
        if not check.is_selected():
            check.click()

        date_selector = self.driver.find_element_by_id("dateSelector1")
        date_selector.find_element_by_css_selector("option[value='from']").click()

        from_date_1 = self.driver.find_element_by_id("fromDate1")
        to_date_1 = self.driver.find_element_by_id("toDate1")

        from_date_1.send_keys(from_date)
        to_date_1.send_keys(to_date)

    def _click_search(self):
        search_button = self.driver.find_element_by_css_selector("input[value='Search']")
        search_button.click()

    def _parse_result(self): # TODO
        frame = self.driver.find_element_by_id('mainFrame')
        self.driver.switch_to.frame(frame)
        element = self.driver.find_element_by_class_name("L0")
        if "No Documents Found" in element.text:
            print("[RESULT - {}]".format(0))
            return 0

        left_frame = self.driver.find_element_by_css_selector("frame[title='Results Classification Frame']")
        self.driver.switch_to.frame(left_frame)
        element = self.driver.find_element_by_class_name("Text3")
        tds = element.find_elements_by_css_selector("td")
        result = tds[0].text
        result = result.strip()
        result = result.replace("(", "").replace(")", "")
        result = int(result)
        print("[RESULT - {}]".format(result))
        return result