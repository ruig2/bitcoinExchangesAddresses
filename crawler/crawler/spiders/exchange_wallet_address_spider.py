import os
import scrapy
import sys
import time


# command to run in terminal: `scrapy crawl exchange_wallet_address`
class ExchangeWalletAddressSpider(scrapy.Spider):
    name = "exchange_wallet_address"

    def start_requests(self):
        url_str = 'https://www.walletexplorer.com/wallet/{}/addresses?page={}'

        with open('../../exchange_names_on_walletexplorer.txt') as fin:
            exchange_names = fin.readlines()
            for exchange_name in exchange_names:
                yield scrapy.Request(url=url_str.format(exchange_name.split()[0], 1), callback=self.parse_first_page)

    def parse_first_page(self, response):
        num_pages = response.css('div.paging a::attr(href)').getall()[-1].split('=')[-1]
        num_pages = int(num_pages)

        page_num = int(response.url.split('=')[-1])
        exchange_name = response.url.split('/')[-2]

        self.log('********* {} : {}/{} *********'.format(exchange_name, page_num, num_pages))

        if not os.path.exists(exchange_name):
            os.makedirs(exchange_name)
        filename_html = '{}/{}-{}.html'.format(exchange_name, exchange_name, page_num)
        with open(filename_html, 'wb') as f:
            f.write(response.body)

        # ToDo: parse to CSV
        rows = response.xpath('//*/table//tr')
        filename_csv = '{}/{}-{}.csv'.format(exchange_name, exchange_name, page_num)
        with open(filename_csv, 'w') as f:
            products = response.xpath('//*/table//tr')
            for product in products[1:]:
                row = []
                for i in range(1, 5):
                    row.append(
                        product.xpath(
                        'td[{}]//text()'.format(i)).extract_first().split(u'\xa0')[0]
                            )
                f.write(','.join(row) + '\n')
        self.log('Saved file %s' % filename_html)

        next_page = None
        if page_num == 1:
            # hrefs are [Next, Last]
            next_page = response.css('div.paging a::attr(href)').getall()[0]
        elif page_num == 2:
            # hrefs are [First, Next, Last]
            next_page = response.css('div.paging a::attr(href)').getall()[1]
        elif page_num > 2 and page_num < num_pages - 1:
            # hrefs are [First, Previous, Next, Last]
            next_page = response.css('div.paging a::attr(href)').getall()[2]
        elif page_num == num_pages - 1:
            # hrefs are [First, Previous, Last]
            next_page = response.css('div.paging a::attr(href)').getall()[2]
        elif page_num == num_pages:
            return

        # Looks like the threshold to ban DDoS access is 1 second on WalletExplorer
        #time.sleep(1)
        if next_page is not None:
            yield response.follow(next_page, self.parse_first_page)
