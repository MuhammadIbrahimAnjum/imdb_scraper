from multiprocessing import Pool, cpu_count
import time
from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd
import sys
sys.setrecursionlimit(1500000)


class ImdbScraper():
    def iterate_pages(self):
        page_items = 51
        start_url = 'https://www.imdb.com/search/title/?countries=in&languages=hi&start=60951&ref_=adv_nxt'
        response = requests.get(start_url)
        with open('imdb_movies_data2.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            headers = ['id', 'name', 'year', 'popularity','length', 'genres']
            writer.writerow(headers) 
            def get_urls(page_items, response):
                soup = BeautifulSoup(response.content, 'html.parser')
                movies = soup.select('.lister-item-content')
                for movie in movies:
                    url = movie.select_one('a')['href']
                    movie_id = url.split('/')[-2]
                    name = movie.select_one('h3 a').text
                    year = movie.select_one('h3 span').text.replace('()','')
                    length = movie.select_one('.runtime')
                    if length:
                        length = length.text
                    else:
                        length = ''
                    genres = movie.select_one('p span[class="genre"]')
                    if genres:
                        genres = genres.text
                    else:
                        genres = ''
                    pg_rating = movie.select_one('.certificate')
                    if pg_rating:
                        pg_rating = pg_rating.text
                    else:
                        pg_rating =''
                    popularity = movie.select_one('.ratings-metascore span')
                    if popularity:
                        popularity = popularity.text
                    else:
                        popularity = ''
                    writer.writerow([movie_id, name, year, popularity,length, genres])
                next_page = f"https://www.imdb.com/search/title/?countries=in&languages=hi&start={page_items}&ref_=adv_nxt"
                print(f"Next Page Url : {next_page}")
                if page_items <= 129591:
                    page_items += 50
                    response = requests.get(next_page)
                    get_urls(page_items, response)
            get_urls(page_items,response)

if __name__ == '__main__':
    imdbscraper = ImdbScraper()
    start_time = time.time()
    imdbscraper.iterate_pages()
    print(f'\n\tTotal Time Taken : {time.time()-start_time}')