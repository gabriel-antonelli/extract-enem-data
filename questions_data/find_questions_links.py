import concurrent.futures
import logging

import requests
from bs4 import BeautifulSoup


class FindQuestionsLinks:
    logging.basicConfig(level=logging.INFO)

    def create_dict_by_year(self, year_area_dict, year, areas):
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                year_area_dict[year] = {}
                url = (
                    "https://REDACTED.com.br/gabarito-enem/questoes/"
                    + str(year)
                    + "/?cor=amarelo&idioma=ingles"
                )

                logging.info(f"Scraping url: {url}")

                response = requests.get(str(url))
                soup = BeautifulSoup(response.text, "html.parser")

                for area in areas:
                    logging.info(f"Searching for {area} in {year}")

                    year_area_dict[year].update({area: []})

                    questions_elements = soup.find_all('a', {'area': area})                    
                    logging.info(f"Found {len(questions_elements)} questions")

                    for link in questions_elements:
                        year_area_dict[year][area].append(link.get("href"))

                return year_area_dict
            except Exception as e:
                logging.error(f"Error processing question: {str(e)}")
                retries += 1
        return year_area_dict

    def find_links(self):
        areas = ["linguagens", "ciencias-natureza", "matematica", "ciencias-humanas"]
        year_area_dict = {}

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [
                executor.submit(
                    self.create_dict_by_year,
                    year_area_dict,
                    year,
                    areas,
                )
                for year in range(2009, 2024)
            ]
            for future in concurrent.futures.as_completed(results):
                year_area_dict = future.result()
        return year_area_dict
