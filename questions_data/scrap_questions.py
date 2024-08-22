import concurrent.futures
import datetime
import logging
import os
import re

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

    @staticmethod
    def extract_question_number( soup):
        question_number_tag = soup.find("p", class_="navigation-question-number")
        if question_number_tag:
            question_number_text = question_number_tag.text.strip()
            match = re.search(r'\d+', question_number_text)
            if match:
                return int(match.group())
        return None

    def download_image(self, img_url, filename):
        img_data = requests.get(img_url).content
        with open(filename, 'wb') as handler:
            handler.write(img_data)

    def add_row(self, soup, context_section, data_list, dir_path):
        question_section = self.get_element_by_classname(
            soup, "section", "alternatives-introduction"
        )
        alternatives_list_ol_text = self.get_element_by_classname(
            soup, "ol", "alternatives-list type-text"
        )
        alternatives_list_ol_image = self.get_element_by_classname(
                soup, "ol", "alternatives-list type-image"
                        )
        answer_div = self.get_element_by_classname(soup, "div", "answer")

        question_text = self.get_elements_text(question_section)
        context_text = self.get_elements_text(context_section)
        alternatives_list = self.transform_elements_text_to_list(alternatives_list_ol_text)
        answer_text = self.transform_elements_text_to_list(answer_div)

        if  all(
            [context_text, question_text, answer_text]
        ):
            question_number = self.extract_question_number(soup)
            # Download and save context images
            context_images = context_section.find_all("img")
            for i, img in enumerate(context_images):
                img_url = img['src']
                os.makedirs(f"{dir_path}/{question_number}-images", exist_ok=True)
                img_filename = f"{dir_path}/{question_number}-images/context_img_{i}.png"
                self.download_image(img_url, img_filename)

            # Download and save alternatives images
            if alternatives_list_ol_image:
                alternatives_images = alternatives_list_ol_image.find_all("img")
                for i, img in enumerate(alternatives_images):
                    img_url = img['src']
                    os.makedirs(f"{dir_path}/{question_number}-images", exist_ok=True)
                    img_filename = f"{dir_path}/{question_number}-images/alt_img_{i}.png"
                    self.download_image(img_url, img_filename)
                    alternatives_list.append(img_filename)
            row = [
                question_number,
                context_text,
                question_text,
                alternatives_list[0] if alternatives_list else None,
                alternatives_list[1] if len(alternatives_list) > 1 else None,
                alternatives_list[2] if len(alternatives_list) > 2 else None,
                alternatives_list[3] if len(alternatives_list) > 3 else None,
                alternatives_list[4] if len(alternatives_list) > 4 else None,
                answer_text[-1].strip() if answer_text else None,
                # Include references to context and alternatives images in CSV
                ",".join(f"{dir_path}/{question_number}-images/context_img_{i}.png" for i in range(len(context_images))),
            ]
            data_list.append(row)

        return data_list

    def process_question(self, link, data_list, dir_path):
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
                        data_list = self.add_row(soup, context_section, data_list, dir_path)
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
                        executor.submit(self.process_question, link, data_list, dir_path)
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
                        "number",
                        "context",
                        "question",
                        "A",
                        "B",
                        "C",
                        "D",
                        "E",
                        "answer",
                        "context-images",
                    ],
                )

                logging.info(f"Saving csv for: {year} in {area}")
                df.to_csv(f"{dir_path}/{area}.csv")
                total_processed += len(data_list)
                logging.info(f"Processed links total: {total_processed}")
        logging.info("Finished processing ENEM data for all years and areas")
        self.log_elapsed_time(start_time)
