import re

import scrapy
from pymongo import MongoClient
from scrapy import Request


def get_id(url):
    return re.match(r"^(https://)?(http://)?www\.imdb\.com/title/(tt[0-9]*)/[a-zA-Z0-9?/_=&]*$", url).group(3)


def get_user_id(url):
    return re.match(r"/user/(ur[0-9]*)/[a-zA-Z0-9?/_=&]*$", url).group(1)
    


class RatingSpider(scrapy.Spider):
    myclient = MongoClient("mongodb://localhost:27017/")
    name = 'imdb_spider'
    data_url = None
    db = myclient["IMDB"]["movies"]
    start_urls = [f"https://www.imdb.com/title/{i['_id']}/reviews/" for i in db.find({})]
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def parse(self, response, **kwargs):
        if response.status is not 404:
            # database
            db = self.myclient["IMDB"]

            # Created or Switched to collection
            # names: GeeksForGeeks
            collection = db["ratings"]
            # iterate movie sections
            ratings = []
            if collection.count_documents({'movie_id': get_id(response.url)}) > 100:
                yield None
            else:
                for rating in response.css(".review-container"):
                    user = ""
                    if "href" in rating.css('.display-name-link a').attrib:
                        user = get_user_id(rating.css('.display-name-link a').attrib["href"])
                    
                    username = rating.css('.display-name-link a::text').get().strip()
                    if collection.count_documents({'_id': f"{get_id(response.url)}_{user}"}) > 0:
                        continue

                    # review title
                    movie_name = rating.css('.title::text').get().strip()
                    # movie year
                    movie_rating = rating.css('.rating-other-user-rating span::text').get() or None
                    if movie_rating is None:
                        continue


                    date = rating.css('.review-date::text').get().strip()
                    text = rating.css('.content .text::text').get().strip()
                    ratings.append({
                        '_id': f"{get_id(response.url)}_{user}",
                        'rating_title': movie_name,
                        'rating': movie_rating,
                        'movie_id': get_id(response.url),
                        'date': date,
                        'text': text,
                        'user_id': user,
                        'username': username
                    })
                if ratings is not None and len(ratings)>0:
                    collection.insert_many(ratings)
                data_key = None
                if "data-key" in response.css('.load-more-data').attrib:
                    data_key = response.css('.load-more-data').attrib["data-key"]
                if "data-ajaxurl" in response.css('.load-more-data').attrib:
                    self.data_url = response.css('.load-more-data').attrib["data-ajaxurl"]
                if data_key is not None and self.data_url is not None:
                    url = f"https://www.imdb.com{self.data_url}?ref_=undefined&paginationKey={data_key}"
                    yield Request(url, callback=self.parse)
                yield None
