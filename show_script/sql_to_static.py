# executes EDA and generates static site on S3

import datetime
import boto

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import pandas as pd
import matplotlib as plt
import seaborn as sns

plt.style.use('fivethirtyeight')
sns.set_style("dark", {'axes.grid' : False})

sc = SparkContext('local[*]')
spark = SparkSession(sc)

# events table
events = spark.read.parquet('s3a://meetupevents/parquet/events/*')
events.take(1)
events.persist(StorageLevel.MEMORY_ONLY)
events.createOrReplaceTempView('events')


# categories table
categories = spark.read.parquet('s3a://meetupevents/parquet/categories/*')
categories.take(1)
categories.persist(StorageLevel.MEMORY_ONLY)
categories.createOrReplaceTempView('categories')


# groups table
groups = spark.read.parquet('s3a://meetupevents/parquet/groups/*')
groups.take(1)
groups.persist(StorageLevel.MEMORY_ONLY)
groups.createOrReplaceTempView('groups')

# venues
venues = spark.read.parquet('s3a://meetupevents/parquet/venues/*')
venues.take(1)
venues.persist(StorageLevel.MEMORY_ONLY)
venues.createOrReplaceTempView('venues')


conn = boto.connect_s3(host="s3.amazonaws.com")
bucket = conn.get_bucket("meetupevents-dashboard")

now = datetime.datetime.today()

# number of events
query = "SELECT COUNT(*) AS total_events FROM events"
num_events = spark.sql(query)

pd_num_events = num_events.toPandas()
num_events_html = pd_num_events.to_html()

num_events_txt = pd_num_events.iloc[0][0]

# number of unique events
query = "SELECT COUNT(DISTINCT event_id) AS unique_events FROM events"
unique_events = spark.sql(query)

pd_unique_events = unique_events.toPandas()
unique_events_html = pd_unique_events.to_html()

unique_events_txt = pd_unique_events.iloc[0][0]

# top 10 event names
query = "SELECT event_name, COUNT(1) AS count \
        FROM events \
        GROUP BY event_name\
        ORDER BY count DESC \
        LIMIT 10"

top10_event_names = spark.sql(query)

# top 10 categories
query = "SELECT category_name, COUNT(1) AS count \
        FROM categories \
        GROUP BY category_name \
        ORDER BY count DESC \
        LIMIT 10"
top10_categories = spark.sql(query)

# top 10 group_names
query = "SELECT group_name, COUNT(1) AS count \
        FROM groups \
        GROUP BY group_name \
        ORDER BY count DESC \
        LIMIT 10"
top10_group_names = spark.sql(query)

# top 10 group cities
query = "SELECT group_city, COUNT(1) AS count \
        FROM groups \
        GROUP BY group_city \
        ORDER BY count DESC \
        LIMIT 10"
top10_group_cities = spark.sql(query)

# top 10 group countries
query = "SELECT group_country, COUNT(1) AS count \
        FROM groups \
        GROUP BY group_country \
        ORDER BY count DESC \
        LIMIT 10"
top10_group_countries = spark.sql(query)

# top 10 venue names
query = "SELECT venue_name, COUNT(1) AS count \
        FROM venues \
        GROUP BY venue_name \
        ORDER BY count DESC \
        LIMIT 10"

top10_venue_names = spark.sql(query)

# top 10 venue cities
query = "SELECT venue_city, COUNT(1) AS count  \
        FROM venues \
        GROUP BY venue_city \
        ORDER BY count DESC \
        LIMIT 10"

top10_venue_cities = spark.sql(query)

# top 10 venue countries
query = "SELECT venue_country, COUNT(1) AS count  \
        FROM venues \
        GROUP BY venue_country \
        ORDER BY count DESC \
        LIMIT 10"

top10_venue_countries = spark.sql(query)

sp_dfs = [top10_event_names, top10_categories, top10_group_names, top10_group_cities, top10_group_countries,
          top10_venue_names, top10_venue_cities, top10_venue_countries]

pd_dfs = []
for df in sp_dfs:
    pd_df = df.toPandas()
    pd_dfs.append(pd_df)

for df in pd_dfs:
    for i, df in enumerate(pd_dfs):
        ax = sns.barplot(y=pd_dfs[i].columns[0], x=pd_dfs[i].columns[1], ci=None, data=df)
        ax.set_title('')
        ax.set_xlabel('')
        ax.set_ylabel('')
        fig = ax.get_figure()
        fig.savefig(str(pd_dfs[i].columns[0])+".svg", bbox_inches='tight', transparent=True)

plot_files = ['category_name.svg', 'event_name.svg',' group_city.svg', 'group_country.svg', 'group_name.svg',
         'venue_city.svg', 'venue_country.svg', 'venue_name.svg']




html_str = """
    <html>

    <body style="text-align:center; width:100%; margin-left:auto; margin-right:auto; margin-top:50px; font-family: Helvetica, Arial, sans-serif;">
    <img style="width:30%;" src="https://secure.meetupstatic.com/s/img/041003812446967856280/logo/svg/logo--script.svg"/>

    <div style="background-color: #ed1c40; color:#fff; width:100%; margin-top:20px; padding-top:2px; padding-bottom:6px;">
    <h2><a href="https://www.meetup.com/meetup_api/docs/stream/2/open_events/" target="blank"  style="color:#fff";>
    OpenEvents Stream</a></h2>

    <p style="text-align:center;">Dashboard for 4/19/2017 to {today}
    </p>
    </div>

    <div style="font-size:20px; text-align: center; width:80%; margin-top:40px; margin-left:auto; margin-right:auto;">

    <p style="color:#777777; line-height: 36px; padding-bottom:20px;">
    Total meetup events
    </br>
    <span style="color:#ed1c40; font-size:30px;">{num_events_txt}</span>
    </p>

    <hr style="border: 0; border-top: 1px solid #eeeeee;">
    <p style="color:#777777; line-height: 36px; padding-top:10px; padding-bottom:20px;">
    Total unique meetup events
    </br>
    <span style="color:#ed1c40; font-size:30px;">{unique_events_txt}</span>
    </p>

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup categories
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot0} />


    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup event names
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot1} />


    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup group cities
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot2} />

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup group countries
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot3} />

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup group names
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot4} />

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup venue cities
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot5} />

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup venue countries
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot6} />

    <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom:40px;">
    <p style="color:#777777">
    Top 10 meetup venue names
    </p>
    <img style="width:100%; margin-bottom:40px;" src={plot7} />

    </div>


    <footer>
    <p style="color:#cccccc">Last Update: {timestamp}</p>
    </footer>
    </body>

    </html>""".format(today=now.strftime("%m/%d/%Y"),num_events_txt=num_events_txt,
                      unique_events_txt=unique_events_txt, plot0=plot_files[0], plot1=plot_files[1],
                      plot2=plot_files[2], plot3=plot_files[3], plot4=plot_files[4], plot5=plot_files[5],
                      plot6=plot_files[6], plot7=plot_files[7], timestamp=now)


html_file = open("meetupevents-report.html","w")
html_file.write(html_str)
html_file.close()


for i, f in enumerate(plot_files):
    output_plot = bucket.new_key(plot_files[i])
    output_plot.content_type = 'image/svg'
    output_plot.set_contents_from_filename(plot_files[i], policy='public-read')

file_key = bucket.new_key('meetupevents-report.html')
file_key.content_type = 'text/html'
file_key.set_contents_from_filename('meetupevents-report.html', policy='public-read')

sc.stop()
