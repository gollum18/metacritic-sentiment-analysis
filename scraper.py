#! /usr/bin/env python3

from db import MongoDB
from cleantext import clean
from scrapy import (
        Spider, Request
)
from uuid import uuid4

root_url = 'https://www.metacritic.com'
genres = [
        'adventure', 'fighting', 'first-person', 'flight', 'party',
        'platformer', 'puzzle', 'racing', 'real-time', 'role-playing',
        'simulation', 'sports', 'strategy', 'third-person', 'turn-based',
        'wargame', 'wrestling', 'action'
]
genre_url = '/browse/games/genre/date/'
query_str = '/all?view=condensed'
critic_reviews = 'critic'
user_reviews = 'user'
batch_size = 100
database = MongoDB()
database.open()
meta_db = database.get_database('metacritic')

def now():
    from datetime import timezone, datetime
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def is_none(d):
    for k, v in d.items():
        if v is None:
            return True
    return False


def clean_dict(d):
    for k, v in d.items():
        if isinstance(v, str):
            d[k] = clean(v, lower=False, no_line_breaks=True)
        elif isinstance(v, list):
            d[k] = list(map(lambda x: clean(x, lower=False, no_line_breaks=True) if isinstance(v, list) else x, v))
        


def print_dict(d):
    print('='*80)
    for k, v in d.items():
        print(f'{k}: {v}')
    print('='*80)


class MetaSpider(Spider):
    name = 'reviews'
    custom_settings = {
        'DOWNLOAD_DELAY': 2
    }

    def start_requests(self):
        for genre in genres:
            url = root_url + genre_url + genre + query_str
            cbargs = {'genre': genre, 'recurse': True}
            yield Request(url, self.parse_genre, cb_kwargs=cbargs)


    def parse_genre(self, response, genre, recurse):
        for td in response.css('td.details'):
            game_url = td.css('a.title::attr(href)').get()
            url = root_url + game_url
            yield Request(url, self.parse_game)
        if recurse and response.css('div.page_nav_wrap'):
            fp = response.css('li.first_page span.page_num::text').get()
            lp = response.css('li.last_page a.page_num::text').get()
            for i in range(int(fp), int(lp) - 1):
                url = root_url + genre_url + genre + query_str + f'&page={i}'
                cbargs = {'genre':genre, 'recurse': False}
                yield Request(url, self.parse_genre, cb_kwargs=cbargs)


    def parse_game(self, response):
        # Parse out game information
        uuid = uuid4()
        title = response.css('div.product_title h1::text').get()
        if response.css('span.platform a'):
            platform = response.css('span.platform a::text').get()
        else:
            platform = response.css('span.platform::text').get()
        publishers = []
        for pub in response.css('li.publisher a::text'):
            publishers.append(pub.get())
        release_date = response.css('li.summary_detail.release_data span.data::text').get()
        metascore = response.css('div.metascore_w.xlarge span::text').get()
        userscore = response.css('div.metascore_w.user::text').get()
        sum_cont = response.css('li.summary_detail.product_summary')
        if sum_cont.css('span.data span.inline_expand_collapse'):
            summary = sum_cont.css('span.data span.inline_expand_collapse span.blurb_expanded::text').get()
        else:
            summary = sum_cont.css('span.data *::text').get()
        developers = []
        if response.css('li.summary_detail.developer span.data'):
            for d in response.css('li.summary_detail.developer span.data::text').get().split(','):
                developers.append(d)
        genres = []
        for genre in response.css('li.summary_detail.product_genre span.data::text'):
            genres.append(genre.get())
        players = response.css('li.summary_detail.product_players span.data::text').get()
        rating = response.css('li.summary_detail.product_rating span.data::text').get()
        game_data = dict()
        game_data['uuid'] = uuid
        game_data['title'] = title
        game_data['platform'] = platform
        game_data['publishers'] = publishers
        game_data['release_date'] = release_date
        game_data['metascore'] = metascore
        game_data['userscore'] = userscore
        game_data['summary'] = summary
        game_data['developers'] = developers
        game_data['genres'] = genres
        game_data['players'] = players
        game_data['rating'] = rating
        game_data['retrieval_ts'] = now()
        if not is_none(game_data):
            clean_dict(game_data)
            meta_db['games'].insert_one(game_data)

        # parse out critic reviews
        if response.css('div.critic_reviews_module p.see_all'):
            critic_link = response.css('div.critic_reviews_module p.see_all a::attr(href)').get()
            url = root_url + critic_link
            kwargs = {'uuid': uuid, 'review_type': critic_reviews, 'review_url': url, 'recurse': True}
            yield Request(url, self.parse_reviews, cb_kwargs=kwargs)
        else:
            reviews = []
            for li in response.css('ol.reviews.critic_reviews li'):
                if li.css('div.source a'):
                    source = li.css('div.source a::text').get()
                else:
                    source = li.css('div.source::text').get()
                grade = li.css('div.review_grade div.metascore_w::text').get()
                date = li.css('div.date::text').get()
                body = li.css('div.review_body::text').get()
                review = dict()
                review['uuid'] = uuid
                review['source'] = source
                review['grade'] = grade
                review['date'] = date
                review['body'] = body
                review['retrieval_ts'] = now()
                if not is_none(review):
                    clean_dict(review)
                    reviews.append(review)
                if len(reviews) >= batch_size:
                    meta_db['critic_reviews'].insert_many(reviews)
                    reviews.clear()
            if len(reviews) > 0:
                meta_db['critic_reviews'].insert_many(reviews)
                reviews.clear()

        # parse out user reviews
        if response.css('div.user_reviews_module p.see_all'):
            user_link = response.css('div.user_reviews_module p.see_all a::attr(href)').get()
            url = root_url + user_link
            kwargs = {'uuid': uuid, 'review_type': user_reviews, 'review_url': url, 'recurse': True}
            yield Request(url, self.parse_reviews, cb_kwargs=kwargs)
        else:
            reviews = []
            for li in response.css('ol.reviews.user_reviews li'):
                if li.css('div.name a'):
                    name = li.css('div.name a::text').get()
                else:
                    name = li.css('div.name span::text').get()
                grade = li.css('div.review_grade div.metascore_w::text').get()
                date = li.css('div.date::text').get()
                if li.css('span.blurb_expanded'):
                    body = li.css('span.blurb_expanded::text').get()
                else:
                    body = li.css('div.review_body span::text').get()
                review = dict()
                review['uuid'] = uuid
                review['user'] = name
                review['grade'] = grade
                review['date'] = date
                review['body'] = body
                review['retreival_ts'] = now()
                if not is_none(review):
                    clean_dict(review)
                    reviews.append(review)
                if len(reviews) >= batch_size:
                    meta_db['user_reviews'].insert_many(reviews)
                    reviews.clear()
            if len(reviews) > 0:
                meta_db['user_reviews'].insert_many(reviews)
                reviews.clear()

    def parse_reviews(self, response, uuid, review_type, review_url, recurse):
        if review_type == critic_reviews:
            reviews = []
            for li in response.css('ol.reviews.critic_reviews li'):
                if li.css('div.source a'):
                    source = li.css('div.source a::text').get()
                else:
                    source = li.css('div.source::text').get()
                date = li.css('div.date::text').get()
                grade = li.css('div.metascore_w::text').get()
                body = li.css('div.review_body::text').get()
                review = dict()
                review['uuid'] = uuid
                review['source'] = source
                review['date'] = date
                review['grade'] = grade
                review['body'] = body
                review['retrieval_ts'] = now()
                if not is_none(review):
                    clean_dict(review)
                    reviews.append(review)
                if len(reviews) >= batch_size:
                    meta_db['critic_reviews'].insert_many(reviews)
                    reviews.clear()
            if len(reviews) > 0:
                meta_db['critic_reviews'].insert_many(reviews)
                reviews.clear()
        elif review_type == user_reviews:
            reviews = []
            for li in response.css('ol.reviews.user_reviews li'):
                if li.css('div.name a'):
                    name = li.css('div.name a::text').get()
                else:
                    name = li.css('div.name span::text').get()
                date = li.css('div.date::text').get()
                grade = li.css('div.metascore_w::text').get()
                if li.css('span.blurb_expanded'):
                    body = li.css('span.blurb_expanded::text').get()
                else:
                    body = li.css('div.review_body span::text').get()
                review = dict()
                review['uuid'] = uuid
                review['name'] = name
                review['date'] = date
                review['grade'] = grade
                review['body'] = body
                review['retrieval_ts'] = now()
                if not is_none(review):
                    clean_dict(review)
                    reviews.append(review)
                if len(reviews) >= batch_size:
                    meta_db['user_reviews'].insert_many(reviews)
                    reviews.clear()
            if len(reviews) > 0:
                meta_db['user_reviews'].insert_many(reviews)
                reviews.clear()
        if recurse and response.css('div.page_nav'):
            fp = response.css('ul.pages li.first_page span::text').get()
            lp = response.css('ul.pages li.last_page a::text').get()
            for i in range(int(fp), int(lp)):
                url = review_url + f"?page={i}"
                kwargs = {'uuid': uuid, 'review_type':review_type, 'review_url': review_url, 'recurse': False}
                yield Request(url, self.parse_reviews, cb_kwargs=kwargs)
