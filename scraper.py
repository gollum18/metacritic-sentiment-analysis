import scrapy

root_url = 'https://www.metacritic.com'
genre_url = root_url + '/browse/games/genre/metascore'
genres = [
	'action', 'adventure', 'fighting',
	'first-person', 'flight', 'party',
	'platformer', 'puzzle', 'racing',
	'real-time', 'role-playing', 'simulation',
	'sports', 'strategy', 'third-person', 
	'turn-based', 'wargame', 'wrestling'
]

class InitSpider(scrapy.Spider):
	'''Implements a scrapy spider that initializes the current review
	database.
	'''
	name = 'init'
	critic_review = 'critic'
	user_review = 'user'

	def start_requests(self):
		print('Requesting page for each genre...')
		# parse each genre
		for genre in genres:
			yield scrapy.Request(
				url=genre_url + f'/{genre}/all?view=detailed',
				callback = self.parse_genre,
				cb_kwargs = dict(genre=genre, recurse=True)
			)

	def parse_genre(self, resp, genre, recurse):
		print(f'Reqeuesting games for genre {genre}...')
		# get a list of all games on the current page
		games = resp.xpath('//table[@class="clamp-list"]//a[@class="title"]/@href')
		# parse each game
		for game in games:
			gname = game.get()
			game_url = root_url + gname
			yield scrapy.Request(
				url = game_url,
				callback = self.parse_game,
			)
		# move to the next game
		if recurse:
			first_page = int(resp.xpath('//ul[@class="pages"]/li[contains(@class, "first_page")]//text()').get()) + 1
			last_page = int(resp.xpath('//ul[@class="pages"]/li[contains(@class, "last_page")]//text()').get())
			for i in range(first_page, last_page):
				url = genre_url + f'/{genre}/all?view=detailed&page={i}'
				yield scrapy.Request(
					url = url,
					callback = self.parse_genre,
					cb_kwargs = dict(genre=genre, recurse=False)
				)

	def parse_game(self, resp):
		# parse game details


		# parse user reviews
		critic_link = resp.xpath('//")]/@href').get()
		if critic_link:
			yield scrapy.Request(
				url = critic_link,
				callback = self.parse_reviews,
				cb_kwargs = dict(review_type=critic_review)
			)
		else:
			critic_list = resp.xpath('//ol[contains(@class, "critic_reviews")]')
			for e in critic_list:
				review_source = e.xpath('//div[@class="source"]/text()').get()
				review_grade = e.xpath('//div[@class="review_grade"]/text()').get()
				review_text = e.xpath('//div[@class="review_body"]').get()
				review_date = e.xpath('//div[@class="date"]/text()').get()
			pass

		# parse critic reviews
		user_link = resp.xpath('///@href').get()
		if user_link:
			yield scrapy.Request(
				url = user_link,
				callback = self.parse_reviews,
				cb_kwargs = dict(review_type=user_review)
			)
		else:
			user_list = resp.xpath('//ol[contains(@class, "user_reviews")]')
			for e in user_list:
				review_source = e.xpath('//div[@class="name"]/text()').get()
				review_grade = e.xpath('//div[@class="review_grade"]/text()').get()
				review_text = e.xpath('//div[@class="review_body"]/text()').get()
				review_date = e.xpath('//div[@class="date"]/text()').get()
				thumbs_helpful = e.xpath('//span[@class="total_ups"/text()]').get()
				thumgs_total = e.xpath('//span[@class="total_thumbs"/text()]').get()

			pass
	
	def parse_reviews(self, resp, review_type):
		
