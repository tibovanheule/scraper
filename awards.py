import re

import scrapy
from pymongo import MongoClient


def get_id(url):
    return re.match(r"^(https://)?(http://)?www\.imdb\.com/[a-z]*/(tt[0-9]*)/[a-zA-Z0-9?/_&=]*$", url).group(3)


class IMDBSpider(scrapy.Spider):
    name = 'imdb_spider'
    myclient = MongoClient("mongodb://localhost:27017/")
    start_urls = [f"https://www.imdb.com/title/{i['_id']}/awards/" for i in myclient["IMDB"]["movies"].find({})]
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def parse(self, response, **kwargs):
        # database
        db = self.myclient["IMDB"]

        # Created or Switched to collection
        # names: GeeksForGeeks
        collection = db["awards"]
        # iterate movie sections
        data = {"_id": get_id(response.url)}
        if collection.count_documents({'_id': data["_id"]}) == 0:
            for awards_list in response.css(".ipc-page-section"):
                if "data-testid" in awards_list.attrib:
                    continue
                subdata = {}
                title_holder = awards_list.css(".ipc-title")
                title = title_holder.css(".ipc-title-link-wrapper span::text").get()
                if title is None:
                    del title, title_holder, subdata
                    continue
                link = title_holder.xpath("a/@href").get()
                del title_holder
                awards = []
                for awardsornominee in awards_list.xpath("div/ul/li"):
                    metadata = awardsornominee.css(".ipc-metadata-list-summary-item__c")
                    title_ = metadata.xpath("div/a/text()").get()
                    for_ = metadata.xpath("div/ul/li/span/text()").extract()
                    people = metadata.xpath("div/ul/li/a/text()").extract()
                    awards.append({"title": title_, "for": for_, "involed": people})
                subdata["link"] = link
                subdata["awards or nominee"] = awards
                data[title] = subdata
            collection.insert_one(data)
            del data
        yield None
