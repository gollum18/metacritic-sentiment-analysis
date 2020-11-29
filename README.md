# metacritic-sentiment-analysis
Project for CIS 660 Data Mining at CSU


Implements a sentiment analysis pipeline with the following stages:
1. Scrapy Metacritic game review scraper
2. Hadoop review cleaner
3. Hadoop review word averager
4. Hadoop review sentiment score generator
5. Review grade estimation and review classification via Tensorflow


This project also implements a Stanford Core NLP pipeline for annotating reviews with sentiment scores, however, for my dataset it always produces sentiment scores of either 0 or 1. I wanted sentiment scores that were in the range \[0, 1\]. Step 3 of the above pipeline determines the average review score per word in each review. Step 4 then takes the average of the average scores of each word in each review as the sentiment score. For critic reviews, this results in sentiment scores in the range \[0, 100\]. These values need scaled to \[0, 1\] before training the estimator and classifier in step 5. This can be easily done with Pandas and sklearn min-max scaling.


Note that the Hadoop job in step 4 is not optimized. The map stage takes a long amount of time to run. If someone wants to replicate this project, take that into account and either optimize it further than I have, or accept it. The other Hadoop jobs do not require near as much time.
