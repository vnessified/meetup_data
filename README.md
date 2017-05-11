# Meetup OpenEvents Stream Data

Stream and analyze event data from meetup.com using the OpenEvents Stream API and AWS products.

[Official Meetup API documentation](https://www.meetup.com/meetup_api/docs/stream/2/open_events/)

[View live OpenEvents Stream](http://stream.meetup.com/2/open_events)

## Architecture
Overview of the data system and tools used.
### Stream
*__Python script utilizing the requests library
*__EC2 runs the above script
*__Kinesis + boto pipe the JSON data to a S3 bucket


![dag](images/dag.png)
