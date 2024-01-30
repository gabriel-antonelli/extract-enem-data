import concurrent.futures
import datetime
import logging
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .find_questions_links import FindQuestionsLinks


class ScrapQuestions:
    @staticmethod
    def create_dir(path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    @staticmethod
    def request_question(link):
        response = requests.get(link)
        return response if response.status_code == 200 else None

    @staticmethod
    def verify_img_question(question):
        return bool(question.find("img"))

    @staticmethod
    def get_elements_text(elements):
        if elements:
            return "\n".join(element.text for element in elements)
        return ""

    @staticmethod
    def get_element_by_classname(soup, tag, classname):
        return soup.select_one(f"{tag}[class='{classname}']")

    @staticmethod
    def transform_elements_text_to_list(lst):
        raw_list = ScrapQuestions.get_elements_text(lst).splitlines()
        return [line for line in raw_list if line.strip()]

    def add_row(self, soup, context_section, data_list):
        question_section = self.get_element_by_classname(
            soup, "section", "alternatives-introduction"
        )
        alternatives_list_ol = self.get_element_by_classname(
            soup, "ol", "alternatives-list type-text"
        )
        answer_div = self.get_element_by_classname(soup, "div", "answer")

        question_text = self.get_elements_text(question_section)
        context_text = self.get_elements_text(context_section)
        alternatives_list = self.transform_elements_text_to_list(alternatives_list_ol)
        answer_text = self.transform_elements_text_to_list(answer_div)

        if len(alternatives_list) == 5 and all(
            [context_text, question_text, answer_text]
        ):
            row = [
                context_text,
                question_text,
                alternatives_list[0],
                alternatives_list[1],
                alternatives_list[2],
                alternatives_list[3],
                alternatives_list[4],
                answer_text[-1][-1],
            ]
            data_list.append(row)

        return data_list

    def process_question(self, link, data_list):
        max_retries = 5
        retries = 0

        while retries < max_retries:
            try:
                response = self.request_question(link)
                if response is not None:
                    soup = BeautifulSoup(response.text, "html.parser")
                    context_section = self.get_element_by_classname(
                        soup, "section", "question-content"
                    )
                    if context_section is not None:
                        verify_img_question = self.verify_img_question(context_section)
                        if verify_img_question is False:
                            data_list = self.add_row(soup, context_section, data_list)
                return data_list
            except Exception as e:
                logging.error(f"Error processing question: {link} - {str(e)}")
                retries += 1
        return data_list

    def log_elapsed_time(self, start_time):
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        total_seconds = int(elapsed_time.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, remainder = divmod(remainder, 60)
        seconds, milliseconds = divmod(remainder, 1)

        logging.info(f"Total time: {hours}h:{minutes}m:{seconds}s:{milliseconds}mm")

    def __init__(self):
        start_time = datetime.datetime.now()
        self.create_dir("enem-data")
        enem_data = FindQuestionsLinks().find_links()
        total_processed = 0
        for year in enem_data:
            dir_path = f"enem-data/enem-{year}"
            self.create_dir(dir_path)
            year_areas = enem_data[year]
            for area in year_areas:
                links = year_areas[area]

                data_list = []

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = [
                        executor.submit(self.process_question, link, data_list)
                        for link in links
                    ]
                    for future in concurrent.futures.as_completed(results):
                        data_list = future.result()
                        logging.info(
                            f"Processed links in {year} for {area}: {len(data_list)}"
                        )
                df = pd.DataFrame(
                    data_list,
                    columns = [
                        "context",
                        "question",
                        "A",
                        "B",
                        "C",
                        "D",
                        "E",
                        "answer",
                    ],
                )

                logging.info(f"Saving csv for: {year} in {area}")
                df.to_csv(f"{dir_path}/{area}.csv")
                total_processed += len(data_list)
                logging.info(f"Processed links total: {total_processed}")
        logging.info("Finished processing ENEM data for all years and areas")
        self.log_elapsed_time(start_time)
