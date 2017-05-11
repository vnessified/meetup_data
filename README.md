# Meetup OpenEvents Stream Data

Stream and analyze event data from Meetup.com using the OpenEvents Stream API and AWS products.

[Official Meetup API](https://www.meetup.com/meetup_api/docs/stream/2/open_events/)

[Live OpenEvents Stream](http://stream.meetup.com/2/open_events)

## Architecture
Overview of the data system and tools used.

### Stream
- Python script utilizing the requests library

- EC2 executes the above script

- Kinesis + boto pipes the data to S3

### Store
- All unstructured data (JSON) is stored on S3

- The JSON files are stored in a year/month/day/hour directory structure

### Structure
- Spark (PySpark) reads raw JSON and transforms it to structured Parquet format, adhering to third normal form (3NF)

- EMR handles the above data transformations

### Synthesize

### Show

## Architecture Diagram
![dag](images/dag.png)
