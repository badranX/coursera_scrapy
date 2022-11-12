import scrapy
import csv
from scrapy.crawler import CrawlerProcess
from urllib.parse import urljoin, urlparse
import sys


MAIN_URL = "https://coursera.org"
browse_url = "https://www.coursera.org/browse/"


class CourseSpider(scrapy.Spider):
    name = "course"

    def __init__(self, url, **kwargs):
        self.url = urljoin(browse_url, url)
        self.titles = set()
        self.browse = True
        super().__init__(**kwargs)

    @classmethod
    def start(cls, category):
        category = category.strip().lower().split()
        category = '-'.join(category)
        process = CrawlerProcess({'LOG_ENABLED':0})
        process.crawl(cls, url=category)
        process.start()

    def start_requests(self):
        with open("out.csv", 'w') as f:
            writer = csv.writer(f)
            header = ['Category Name','Course Name','First Instructor Name','Course Description','# of Students Enrolled','# of Ratingas', 'Item Type']
            writer.writerow(header)

        yield scrapy.Request(url=self.url, callback=self.parse_main)

    def parse_main(self, response):
        tmp = response.css('div.rc-CollectionItem-wrapper')
        hrefs = tmp.css('a::attr(href)')
        hrefs = [urljoin(MAIN_URL, u.get()) for u in hrefs]
        hrefs = set(hrefs)
        for i, url in enumerate(hrefs):
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        data = {}
        path = urlparse(response.url).path
        item_type = path.split('/')[1]
        data['item_type'] = item_type

        h1 = response.css('h1')
        title = h1.css('::text').get()
        data['title'] = title

        if title not in self.titles:
            head = h1.xpath('..')
            description = head.css('p::text').get()
            data['description'] = description

            instr = response.css('div.rc-BannerInstructorInfo')
            instructor = instr.css('span::text').get()
            data['instructor'] = instructor

            metric = response.css('div.rc-ProductMetrics')
            number_of_enrolled = metric.css('span::text').get()
            data['number_of_enrolled'] = number_of_enrolled

            rating_count = response.css('[data-test*="ratings-count"]')
            rating_count = rating_count.css('span::text').getall()
            if rating_count:
                rating_count = [x for x in rating_count if x.split()[-1].strip() == 'ratings']
                rating_count = rating_count[0].split()[0]
            data['rating_count'] = rating_count if rating_count else None

            main = response.css('#main')
            role = main.css('div[role="navigation"]')
            category = role.css('a::text')
            if category:
                category = category[-1].get()
            data['category'] = category if category else None

            print("finished: ", data['title'])

            with open("out.csv", 'a') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data['category'],
                    data['title'],
                    data['instructor'],
                    data['description'],
                    data['number_of_enrolled'],
                    data['rating_count'],
                    data['item_type']])


if __name__ == "__main__":
    arg = sys.argv[-1]
    CourseSpider.start(arg)

    #upload file
    import pandas as pd
    import requests

    df = pd.read_csv('out.csv')
    df.to_html('out.html')

    csvfile = open("out.html", "rb")
    url = "http://badranx.pythonanywhere.com/action"
    test_response = requests.post(url, files = {"file": csvfile})
    print("server response: ", test_response)
    print("You can check the csv on: ")
    print("http://badranx.pythonanywhere.com/out.html")
