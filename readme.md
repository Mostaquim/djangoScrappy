# TODO
- Create job database
- Make scheduler script `save job id db->job->job`
- Enable spider to receive scrapyd vars  `use post request in php curl` 
- List scrapyd schedule 
- Canceling spiders from interface `web/spider/schedule`
- Remove the previous save states functions
- The keywords should be received by scrapyd [arguements](https://stackoverflow.com/questions/15611605/how-to-pass-a-user-defined-argument-in-scrapy-spider)
`$ curl http://localhost:6800/schedule.json -d project=myproject -d spider=somespider -d setting=DOWNLOAD_DELAY=2 -d arg1=val1`
- Database should store jobs


##On scrapyd start
1. Get all incomplete jobs from the db and schedule them


####Searchpage Spider
Parameters from scrapyd: job

On start: 
```
get job details from db: keyword
update status -> 1
```
Save product `asin, price, rating , review, image`

End condition: if no next url

Analysis before end: 
````
if number of products above 600 review is below 10
db: create new job with product spider
scrapyd: schedule the job 
````


####Product Spider
Parameters from scrapyd: job, searchBy

On start: 

````
get job details from db: keyword, product
update job status -> 1
if searchby is keyword: get asins by keyword
else: get asins by product
````
Save product: `category, bsr, weight, dimension`

##Web interface
#### Dashboard
- Show Active search
- Scrapyd status


#### Keywords
- Show Current keywords `crawled(yes/no), view`
- View `Scrapped product and details`
- Add new keyword


#### Products
- Product Listing `Number, Stats`
- Add products from keyword
- Remove unwanted product
- Assign new keyword for the product and find next
````
Assign new keyword function
Add new keyword
Add the keyword as job
Add the job to scrapyd scheduler
````
- **Function** Queue product spider
```
Add job to db with searcby == product, product = product_id
Add the job to scheduler
```
- **Function** Generate statistics (requires all results)
```
Categorize product red, yellow, green according to criteria
```
- **Function** Generate keywords
```
Get all the titles from all the products
Find the most used words and suggest for next keywords
```


##Database
###job
```
|ID|job|param|searchby|status|
ID -> job_id
job -> id from scrapyd
param -> keyword/url
searchby -> ('url','keyword','product')
status -> (0 -> untouched, 1-> started crawling, 2-> crawling_finish)
```

##Questions
How to automate schedule generation?
`Run a service?`

Why would'nt the schedule will finish and the scraper stop?

Automate keyword generation from results?

How would the crawler comeup with new products?



##Conflicts:
Currently none of the spider generating any new keywords

## Features to work on:
- keyword suggestion
- background service for notification
