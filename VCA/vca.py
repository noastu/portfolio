"""
This module provides a class, VCAScrape, for scraping information about dog breeds from the VCA Hospitals website.
"""

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime

class VCAScrape:
    """
    A class for scraping information about dog breeds from the VCA Hospitals website.
    """

    root_page = 'https://vcahospitals.com/'

    def __init__(self, export_path):
        """
        Initializes a VCAScrape object with the specified export path.

        Args:
            export_path (str): The directory path where scraped data will be exported.
        """
        self.export_path = export_path

    def get_breeds(self):
        """
        Retrieves links to individual breed pages from the main dog breeds page of the VCA Hospitals website.

        Returns:
            bool: True if successful.
        """
        resp = requests.get(VCAScrape.root_page + 'know-your-pet/dog-breeds')
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        links = soup.find_all('a', {'class': 'breed-list__item__link'}, href=True)
        breed_pages = [link['href'] for link in links]
        self.breed_pages = breed_pages
        return True
    
    def get_details(self):
        """
        Scrapes detailed information about each dog breed from their respective pages.

        Returns:
            list: A list of dictionaries containing details about each breed.
        """
        breed_details = []
        for breed in tqdm(self.breed_pages, desc='Dog Breed', total=len(self.breed_pages)):
            breed_details.append(self._breed_info(breed))
        self.details = breed_details
    
    def _breed_info(self, link):
        """
        Scrapes detailed information about a single dog breed from its page.

        Args:
            link (str): The URL of the breed's page.

        Returns:
            dict: A dictionary containing details about the breed.
        """
        breed = {} 
        resp = requests.get(VCAScrape.root_page + link)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        breed_id =  link.rsplit('/', 1)[1]
        breed['breed'] = breed_id
        breed['details'] = self._breed_details(soup)
        breed['stats'] = self._breed_stats(soup)
        breed['traits'] = self._breed_traits(soup)
        breed['created_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return breed
    
    def _breed_details(self, soup):
        """
        Extracts and cleans detailed information about a breed from its page.

        Args:
            soup: BeautifulSoup object representing the breed's page.

        Returns:
            str: Detailed information about the breed.
        """
        details = soup.find('div', {'class': 'kyp-breed-detail__content'})
        details_clean = ' '.join([val.text.strip() for val in details])
        return details_clean
    
    def _breed_stats(self, soup):
        """
        Extracts and cleans statistical information about a breed from its page.

        Args:
            soup: BeautifulSoup object representing the breed's page.

        Returns:
            dict: Statistical information about the breed.
        """
        titles = soup.find_all('div', {'class': 'kyp-breed-detail__stats__label sm-text'})
        titles_text = [val.text.strip() for val in titles]
        values = soup.find_all('div', {'class': 'kyp-breed-detail__stats__value'})
        values_text = [val.text.strip() for val in values]
        stats = zip(titles_text, values_text)
        stats_clean = {}
        for l in list(stats):
            stats_clean[l[0]] = l[1]
        return stats_clean
    
    def _breed_traits(self, soup):
        """
        Extracts and cleans trait information about a breed from its page.

        Args:
            soup: BeautifulSoup object representing the breed's page.

        Returns:
            dict: Trait information about the breed.
        """
        labels = soup.find_all('span', {'class': 'kyp-breed-detail__traits__label'})
        labels_text = [val.text.strip() for val in labels]
        ratings = soup.find_all('div', {'class': 'kyp-breed-detail__traits__value'})
        ratings_text = [val.text.strip() for val in ratings]
        traits = zip(labels_text, ratings_text)
        traits_clean = {}
        for l in tuple(traits):
            traits_clean[l[0]] = l[1]
        return traits_clean