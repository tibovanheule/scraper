import re

import scrapy
from pymongo import MongoClient


def get_id(url):
    return re.match(r"^(https://)?(http://)?www\.imdb\.com/title/(tt[0-9]*)/[a-zA-Z0-9?/_=&]*$", url).group(3)


class ParentalSpider(scrapy.Spider):
    myclient = MongoClient("mongodb://localhost:27017/")
    name = 'imdb_spider'
    data_url = None
    db = myclient["IMDB"]["movies"]
    start_urls = [f"https://www.imdb.com/title/{i['_id']}/parentalguide" for i in db.find({})]
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def parse(self, response, **kwargs):
        if response.status is not 404:
            # database
            db = self.myclient["IMDB"]

            # Created or Switched to collection
            # names: GeeksForGeeks
            collection = db["parental"]
            # iterate movie sections

            if collection.count_documents({'movie_id': get_id(response.url)}) > 0:
                yield None
            else:
                data = {'_id': get_id(response.url)}
                certifications = response.xpath("//*[contains(@id,'certifications-list')]/td/ul/li/a/text()").extract()
                for i in certifications:
                    data[i.split(":")[0]] = i.split(":")[1]
                data["sex_nudity"] = response.xpath("//*[contains(@id,'advisory-nudity')]/ul/li/div/label/div/div/span/text()").extract()
                data["alcohol"] = response.xpath(
                    "//*[contains(@id,'advisory-alcohol')]/ul/li/div/label/div/div/span/text()").extract()
                data["Violence"] = response.xpath(
                    "//*[contains(@id,'advisory-violence')]/ul/li/div/label/div/div/span/text()").extract()
                data["profanity"] = response.xpath(
                    "//*[contains(@id,'advisory-profanity')]/ul/li/div/label/div/div/span/text()").extract()
                data["frightening"] = response.xpath(
                    "//*[contains(@id,'advisory-frightening')]/ul/li/div/label/div/div/span/text()").extract()

                if data is not None:
                    collection.insert_many([data])
            yield None
