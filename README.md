# Scraper for IMDB using Scrapy project
## Requirements
MongoDB to insert the generated documents, preferably on localhost for fast inserting.
## Deployment

### run

`scrapy runspider movies.py`

after that

`scrapy runspider ratings.py`

### important optimalisations

1. create an index on movie_id in the ratings table. When the number of documents grows, this ensures that the count of documents functions fast.
