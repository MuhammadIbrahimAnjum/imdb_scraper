import logging
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
        start_url = 'https://www.imdb.com/search/title/?countries=in&languages=hi'
        response = requests.get(start_url)
        def get_urls(page_items, response):
            soup = BeautifulSoup(response.content, 'html.parser')
            movies = soup.select('.lister-item-content')
            for movie in movies:
                url = movie.select_one('a')['href']
                name = movie.select_one('h3 a').text
                names.append(name)
                length = movie.select_one('.runtime')
                if length:
                    length = length.text
                    lengths.append(length)
                else:
                    length = ''
                    lengths.append(length)
                genres = movie.select_one('p span[class="genre"]')
                if genres:
                    genres = genres.text
                    generes_list.append(genres)
                else:
                    genres = ''
                    generes_list.append(genres)
                pg_rating = movie.select_one('.certificate')
                if pg_rating:
                    pg_rating = pg_rating.text
                    pg_ratings.append(pg_rating)
                else:
                    pg_rating =''
                    pg_ratings.append(pg_rating)
                popularity = movie.select_one('.ratings-metascore span')
                if popularity:
                    popularity = popularity.text
                    popularities.append(popularity)
                else:
                    popularity = ''
                    popularities.append(popularity)
                link = f"https://www.imdb.com{url}"
                url_list.append(link)
            next_page = f"https://www.imdb.com/search/title/?countries=in&languages=hi&start={page_items}&ref_=adv_nxt"
            print(f"Next Page Url : {next_page}")
            if page_items <= 129591:
                page_items += 50
                response = requests.get(next_page)
                get_urls(page_items, response)
        get_urls(page_items,response)
        
    def parse_details(self,index, url):
        try:
            response = requests.get(url,headers={'User-Agent': 'Mozilla/5.0'})
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                movie_id = url.split('/')[-2]
                imdb_rating = ''
                imdb_rating = soup.select_one('div[data-testid="hero-rating-bar__aggregate-rating__score"] span:nth-child(1)')
                if imdb_rating:
                    imdb_rating = imdb_rating.text
                video_trailer_link = ''
                video_trailer = soup.select_one('div[data-testid="shoveler-items-container"] a[data-testid="videos-slate-overlay-1"]')
                if video_trailer:
                    video_trailer = video_trailer['href']
                    video_trailer_link = f"https://www.imdb.com{video_trailer}"
                year = ''
                year = soup.select_one('div[data-testid="title-details-section"] li:nth-child(1) a+div')
                if year:
                    year = year.text
                    if ',' in year:
                        year = year.split(',')[1]
                        if '(' in year:
                            year = year.split('(')[0]
                poster_url = ''
                poster_url = soup.select_one('div[data-testid="hero-media__poster"] a')['href']
                poster_url = f"https://www.imdb.com{poster_url}"
                summary = ''
                summary = soup.select_one('p[data-testid="plot"]')
                if summary:
                    summary = summary.text
                full_cast_crew_link = f"{url}fullcredits"
                response = requests.get(full_cast_crew_link,headers={'User-Agent': 'Mozilla/5.0'})
                writers = []
                casts = []
                directors = []
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    directors_tag = soup.select('h4[id="director"]+table a')
                    for director in directors_tag:
                        director_name = director.text.replace('\n',"")
                        directors.append(director_name)
                    writers_table = soup.select('h4[id="writer"]+table tbody tr td[class="name"]')
                    for movie_writer in writers_table:
                        writer_name = movie_writer.select_one('a').text.replace('\n',"")
                        writers.append(writer_name)
                    cast_table = soup.select('table[class="cast_list"] tr td:nth-child(2) a')
                    for cast in cast_table:
                        cast_name = cast.text.replace('\n',"")
                        casts.append(cast_name)
                idx = index
                print(f"Passed {idx}, {url}")
                return idx, movie_id , year, imdb_rating, summary, directors, casts,  writers,  video_trailer_link, poster_url
       
        except Exception as e:
            idx = index
            print(f"Failed {idx}, {url}")
            logging.critical(e, exc_info=True)
            return idx, url

if __name__ == '__main__':
    imdbscraper = ImdbScraper()
    start_time = time.time()
    url_list = []
    names = []
    lengths = []
    generes_list = []
    pg_ratings = []
    popularities = [] 
    imdbscraper.iterate_pages()
    with open('imdb_movies_data.csv', 'w', newline='') as data_file, open('failed_urls.csv', 'w', newline='') as data_failed_files:
        writer1 = csv.writer(data_file)
        data_headers = ['id', 'name', 'year', 'imdb_rating', 'popularity', 'summary', 'length', 'genres', 'directors', 'actors', 'writers', 'pg_rating','trailer_url', 'poster_url']
        writer1.writerow(data_headers)
        writer2 = csv.writer(data_failed_files)
        failed_headers = ['id', 'url', 'name', 'popularity', 'length', 'genres', 'pg_rating']
        writer2.writerow(failed_headers)
        with Pool(cpu_count()) as process:
            data_recieved = process.starmap(imdbscraper.parse_details, enumerate(url_list))
            for item in data_recieved:
                print(len(item))
                if len(item) > 2:
                    idx = item[0]
                    name = names[idx]
                    movie_id = item[1]
                    year = item[2]
                    imdb_rating = item[3]
                    summary = item[4]
                    directors = item[5]
                    actors = item[6]
                    writers = item[7]
                    video_trailer_link = item[8]
                    poster_url = item[9]
                    popularity = popularities[idx]
                    length = lengths[idx]
                    genres = generes_list[idx]
                    pg_rating = pg_ratings[idx]
                    writer1.writerow([movie_id, name, year, imdb_rating, popularity, summary, length, genres, directors, actors, writers, pg_rating, video_trailer_link,poster_url])

                if len(item) <= 2:
                    idx = item[0]
                    name = names[idx]
                    url = item[1]
                    popularity = popularities[idx]
                    length = lengths[idx]
                    genres = generes_list[idx]
                    pg_rating = pg_ratings[idx]
                    writer2.writerow([idx, url, name, popularity, length, genres, pg_rating])
                    
    data_file.close()
    data_failed_files.close()
    print(f'\n\tTotal Time Taken : {time.time()-start_time}')