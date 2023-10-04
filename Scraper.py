# Import libraries
from bs4 import BeautifulSoup


import requests
import pandas as pd
import numpy as np
import re
from tqdm import tqdm


class Scraper:
    def __init__(self, user_agent=None):
        self.user_agent = user_agent

    def __build_url(self, query):
        url_encoded_query = query.replace(" ", "%20")
        return (
            f"https://ikman.lk/en/ads/sri-lanka?"
            f"sort=relevance"
            f"&order=desc"
            f"&buy_now=0"
            f"&urgent=0"
            f"&query={url_encoded_query}"
        )

    def scrape(self, query, num_pages=1):
        """Scrape the ikman.lk page for given query and number of pages

        Args:
            query (string): search query.
            num_pages: number of pages to be scraped for the given query.

        Yields:
            Dataframe: A pandas dataframe containing the scraped data.
        """

        ad_list = []

        for num_page in tqdm(range(1, num_pages + 1)):
            results_page = requests.get(self.__build_url(query) + f"&page={num_page}")
            results_parsed = BeautifulSoup(results_page.content, "html.parser")
            ads = results_parsed.find_all("a", attrs={"class": "gtm-ad-item"})

            for ad in tqdm(ads):
                title = ad.find("h2").get_text()
                link = ad["href"]
                premium_member = (
                    "member"
                    if ad.find("div", attrs={"class", re.compile(r"^premium-member")})
                    else "not a member"
                )
                verified_member = (
                    "verified"
                    if ad.find(
                        "div", attrs={"class", re.compile(r"^verified-badge-container")}
                    )
                    else "not verified"
                )
                location = (
                    ad.find("div", attrs={"class", re.compile(r"^description")})
                    .get_text()
                    .split(",")[0]
                )
                category = (
                    ad.find("div", attrs={"class", re.compile(r"^description")})
                    .get_text()
                    .split(",")[1]
                    .strip()
                )
                price = ad.find(
                    "div", attrs={"class", re.compile(r"^price")}
                ).get_text()
                top_ad = (
                    "top ad"
                    if ad.find("div", attrs={"class", re.compile(r"^top-ad")})
                    else "normal ad"
                )
                img = ad.find("img")["src"]

                ad_dict = {
                    "title": title,
                    "link": link,
                    "premium_member": premium_member,
                    "verified_member": verified_member,
                    "location": location,
                    "category": category,
                    "price": price,
                    "top_ad": top_ad,
                    "img": img,
                    "page_no": num_page,
                }

                ad_list.append(ad_dict)

        return pd.DataFrame(ad_list)