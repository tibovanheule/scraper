import re

import scrapy
from pymongo import MongoClient
from scrapy import Request


def get_id(url):
    return re.match(r"^(https://)?(http://)?www\.imdb\.com/[a-z]*/(tt[0-9]*)/[a-zA-Z0-9?/_&=]*$", url).group(3)


class IMDBSpider(scrapy.Spider):
    name = 'imdb_spider'
    update = True
    myclient = MongoClient("mongodb://localhost:27017/")
    if update:
        start_urls = [f"https://www.imdb.com/title/{i['_id']}/?ref_=undefined" for i in myclient["IMDB"]["movies"].find({})]
    else:
        start_urls = ("https://www.imdb.com/title/tt20850406/?ref_=tt_sims_tt_t_1", "https://www.imdb.com/title/tt0241527/",
                      "https://www.imdb.com/title/tt0266543/?ref_=hm_tpks_tt_i_12_pd_tp1_pbr_ic",
                      "https://www.imdb.com/title/tt2788316/?ref_=hm_top_tt_i_5",
                      "https://www.imdb.com/title/tt10919420/?ref_=hm_tpks_tt_i_15_pd_tp1_pbr_ic",
                      "https://www.imdb.com/title/tt0382932/?ref_=hm_tpks_tt_i_18_pd_tp1_pbr_ic",
                      "https://www.imdb.com/title/tt1632701/?ref_=hm_tpks_tt_i_26_pd_tp1_pbr_ic",
                      "https://www.imdb.com/title/tt14230458/?ref_=hm_top_tt_i_2",
                      "https://www.imdb.com/title/tt15239678/?ref_=hm_top_tt_i_3",
                      "https://www.imdb.com/title/tt0068646/?ref_=nv_sr_srsg_3_tt_4_nm_4_q_godf",
                      "https://www.imdb.com/title/tt0108052/?ref_=hm_tpks_tt_i_4_pd_tp1_pbr_ic")
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        # database
        db = self.myclient["IMDB"]

        # Created or Switched to collection
        # names: GeeksForGeeks
        collection = db["movies2"]
        # iterate movie sections
        data = {"_id": get_id(response.url)}
        if update or collection.count_documents({'_id': data["_id"]}) == 0:
            data['title'] = response.css('.hero__primary-text::text').get().strip()

            genres = response.xpath(
                '//*[@data-testid="genres"]/div[contains(@class,"ipc-chip-list__scroller")]/a/span/text()').extract() or None
            if genres:
                data['genre'] = [genre.strip() for genre in genres]
            data['description'] = response.css('.hlbAws::text').get() or None
            directors = response.xpath(
                "//li[contains(.//span, 'Director')]/div/ul/li/a/text()").extract() or None
            if directors:
                data['directors'] = list(set(directors))
            data["type"] = response.xpath(
                "//meta[contains(@property, 'og:type')]/@content").extract() or None
            data["image"] = response.xpath(
                "//meta[contains(@property, 'og:image')]/@content").extract() or None
            data["origin"] = response.xpath(
                "//*[contains(.//*, 'Country of origin')]/div/ul/li/a/text()").extract() or None
            data["Runtime"] = response.xpath(
                "//*[contains(.//*, 'Runtime')]/div/text()").extract() or None
            data["release date"] = response.xpath(
                "//*[contains(.//*, 'Release date')]/div/ul/li/a/text()").extract() or None

            writers = response.xpath(
                '//*[contains(.//*, "Writers")]/div/ul/li/a/text()').extract() or None
            if writers:
                data['writers'] = list(set(writers))

            stars = response.xpath(
                "//*[contains(.//*, 'Stars')]/div/ul/li/a/text()").extract() or None
            if stars:
                data['stars'] = list(set(stars))
            if update:
                id = data["_id"]
                del data["_id"]
                collection.update_one({"_id":id},{"_set":data},{upsert:True})
            else:
                collection.insert_one(data)
            del data
            if not update:
                for link in response.css(".ipc-poster-card__title"):
                    url = f"https://www.imdb.com{link.attrib['href']}"
                    id = get_id(url)
                    if collection.count_documents({'_id': id}) < 1:
                        del id
                        yield Request(url, callback=self.parse)
        yield None
