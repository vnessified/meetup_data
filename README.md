# Meetup OpenEvents Stream Data

Stream and analyze event data from Meetup.com using the OpenEvents Stream API and AWS products.

[Official Meetup API](https://www.meetup.com/meetup_api/docs/stream/2/open_events/)

[Live OpenEvents Stream](http://stream.meetup.com/2/open_events)

## Architecture
Overview of the data system and tools used.

![dag](images/dag.png)

### Stream
- Python script utilizing the requests library

- Kinesis + boto pipes the data to S3

- EC2 executes the above script


### Storage
- All unstructured data (JSON) stored on S3

- JSON files are stored in a year/month/day/hour directory structure

### Structure
- Spark (PySpark) reads raw JSON and transforms it to structured Parquet format, adhering to third normal form (3NF)

- EMR handles the above data transformations - a cluster is programmatically spun up every hour via a script running on an EC2 instance to ETL new data

- The structured data is then also stored on S3

### Synthesize
- Spark SQL is used to query temporary table like structures created from the Parquet files

- Simple analysis on event data - for example, counts of top meetup categories, cities, and countries

- Machine Learning performed using Spark ML implementing a Random Forest Classifier to predict group category ("outdoor/adventure" vs "tech") using the description text as the feature matrix

### Show
- Spark dataframes are plotted by way of Pandas and Seaborn

- Boto interfaces with AWS and connects the HTML and plots generated to a S3 bucket that will host the static page to serve as a dashboard (see below screenshot)

![dashboard](images/dashboard.png.png)
[View Dashboard](https://s3.amazonaws.com/meetupevents-dashboard/meetupevents-report.html)
