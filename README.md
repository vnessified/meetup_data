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
- All unstructed data (JSON) is stored on S3

- The JSON files are stored in a year/month/day/hour directory structure

### Structure

![dag](images/dag.png)
