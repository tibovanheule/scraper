import scrapy
from pymongo import MongoClient
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule


class RatingSpider(scrapy.Spider):
    myclient = MongoClient("mongodb://localhost:27017/")
    name = 'imdb_spider'
    data_url = None

    start_urls = ("https://www.imdb.com/title/tt20850406/reviews/",)

    def parse(self, response):
        if response.status is not 404:
            # database
            db = self.myclient["IMDB"]

            # Created or Switched to collection
            # names: GeeksForGeeks
            collection = db["ratings"]
            # iterate movie sections
            ratings = []
            for rating in response.css(".review-container"):
                # review title
                movie_name = rating.css('.title::text').get().strip()
                # movie year
                movie_rating = rating.css('.rating-other-user-rating span::text').get().strip()

                user = rating.css('.display-name-link a::text').get().strip()

                date = rating.css('.review-date::text').get().strip()
                text = rating.css('.content .text::text').get().strip()
                movie_dict = {
                    'rating_title': movie_name,
                    'rating': movie_rating,
                    'date': date,
                    'text': text,
                    'user': user
                }
                ratings.append(movie_dict)
            del rating
            collection.insert_many(ratings)
            del ratings, collection
            data_key = None
            if "data-key" in response.css('.load-more-data').attrib:
                data_key = response.css('.load-more-data').attrib["data-key"]
            if "data-ajaxurl" in response.css('.load-more-data').attrib:
                self.data_url = response.css('.load-more-data').attrib["data-ajaxurl"]
            if data_key is not None and self.data_url is not None:
                url = f"https://www.imdb.com{self.data_url}?ref_=undefined&paginationKey={data_key}"
                yield Request(url, callback=self.parse)
