########
# PREP #
########

from pytrends.request import TrendReq
import pandas
import datetime
import tqdm
import google_trends_functions as gtf

keywords = ['tv series', 'tv show', 'film', 'movie']

kw_exclusions = {
                 'tv series': ['2019', 'netflix', 'top', 'best',
                               'watch', '2018', 'bbc', 'new'],
                 'tv show': [],
                 'film': ['2018', 'favourite', '2019', 'camera', 'news',
                          'netflix', 'window', 'times', 'cling', 'home',
                          'cinema', 'polaroid', 'instax', 'izle'],
                 'movie': ['box', '123', 'news', 'house', 'movies',
                           'cinema', 'putlocker', 'hindi']
                 }

kw_dic, kw_list = gtf.kw_prep(keywords, kw_exclusions)


# Connect to Google
pytrends = TrendReq(hl='en-UK',
                    tz=-60,
                    timeout=(25, 25),
                    retries=5,
                    backoff_factor=2)


##################
# MAIN DASHBOARD #
##################

# Define window for back fill

start_date = "2019-01-01"
stop_date = datetime.date.today().strftime("%Y-%m-%d")

start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
stop = datetime.datetime.strptime(stop_date, "%Y-%m-%d")

date_list = []
while start < stop:
    date_list.append(start.strftime("%Y-%m-%d"))
    start = start + datetime.timedelta(days=1)

Backfill_df = pandas.DataFrame()

# Iterates through each backfill date

for dt in tqdm.tqdm_notebook(date_list):

    start = datetime.datetime.strptime(dt, "%Y-%m-%d")

    # Define our date_window of interest - the last full week
    t = gtf.prev_week(start)

    # Build pytrends payload - returns an object
    pytrends.build_payload(kw_list, cat=0, timeframe=t, geo='GB', gprop='')

    # Call method of interest on our pytrends payload object
    df = pytrends.related_queries()

    Backfill_df = gtf.refactor_data(kw_dic,kw_exclusions,t,start,df, Backfill_df)   

Backfill_df.to_gbq(destination_table='TSL03.Google_Trends', project_id='skyuk-uk-nowtv-analytics-prod', chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False, table_schema=None, location=None, progress_bar=False, credentials=None, verbose=None, private_key=None)      


sky_project = "skyuk-uk-nowtv-analytics-prod"

sql = """
        select
        *
        from `skyuk-uk-nowtv-analytics-prod.TSL03.Google_Trends`

        order by
        date_from asc
        , date_to
        , day_run asc
      """

output_df = pandas.read_gbq(sql, project_id=sky_project, dialect='standard')

dates = output_df['date_from'].unique()

tableau_df = pandas.DataFrame()

for i in dates:
    partitioned_df = output_df[output_df['date_from'] == i]

    tableau_df = gtf.agg_table_refactor(partitioned_df, kw_dic, tableau_df)

tableau_df = tableau_df.reset_index()

tableau_df = tableau_df[['query', 'key_word', 'date_from', 'date_to', 'value', 'rank']]

tableau_df.to_gbq(destination_table='TSL03.Google_Trends_Tableau',
                  project_id='skyuk-uk-nowtv-analytics-prod',
                  chunksize=None,
                  reauth=False,
                  if_exists='replace',
                  auth_local_webserver=False,
                  table_schema=None,
                  location=None,
                  progress_bar=False,
                  credentials=None,
                  verbose=None,
                  private_key=None)


#####################################################
# BUILDING THE 'QUERY INTEREST OVER TIME' DASHBOARD #
#####################################################

IOT_df = pandas.DataFrame()

dt_dic = {}

periods = ['week', 'month', 'quarter', 'half', 'year']

for i in periods:

    d1, d2 = gtf.date_points(i)

    dt = d1+' '+d2

    dt_dic[i] = dt

for k in keywords:

    for period, dt in dt_dic.items():

        try:
            pytrends.build_payload([k], cat=0, timeframe=dt, geo='GB', gprop='')

            # Call method of interest on our pytrends payload object

            df = pytrends.interest_over_time()

            df['key_word'] = k.title()

            df['value'] = df[k]

            df['period'] = period.title()

            df['date_from'] = dt.split()[0]
            df['date_to'] = dt.split()[1]

            df = df.reset_index()

            df = df[['key_word', 'date_from', 'date_to', 'date', 'value', 'period']]

        except:
            print('Failed on:', k, 'across the', period+":", dt)

        IOT_df = IOT_df.append(df)

IOT_df.to_gbq(destination_table='TSL03.Google_Trends_IOT',
              project_id='skyuk-uk-nowtv-analytics-prod',
              chunksize=None,
              reauth=False,
              if_exists='replace',
              auth_local_webserver=False,
              table_schema=None,
              location=None,
              progress_bar=False,
              credentials=None,
              verbose=None,
              private_key=None)


###########################################
# INTEREST OVER TIME DEEP DIVE DASHBOARDS #
###########################################

Daily_df = gtf.iot_periods('half',
                           'daily',
                           1,
                           pytrends,
                           kw_list,
                           kw_dic,
                           kw_exclusions)

Weekly_df = gtf.iot_periods('year',
                            'weekly',
                            6,
                            pytrends,
                            kw_list,
                            kw_dic,
                            kw_exclusions)

tableau_df = pandas.DataFrame()

Agg_Df = Weekly_df.append(Daily_df)

Deduped_Agg_DF = Agg_Df[['date_from', 'date_to']].drop_duplicates()

for df, dt in Deduped_Agg_DF.itertuples(index=False):

    partitioned_df = Agg_Df[(Agg_Df['date_from'] == df) & (Agg_Df['date_to'] == dt)]

    tableau_df = gtf.agg_table_refactor(partitioned_df, kw_dic, tableau_df)

tableau_df = tableau_df.reset_index()

tableau_df = tableau_df[['query', 'key_word', 'period', 'date_from', 'date_to', 'value', 'rank']]

tableau_df.to_gbq(destination_table='TSL03.Google_Trends_IOT_DD',
                  project_id='skyuk-uk-nowtv-analytics-prod',
                  chunksize=None,
                  reauth=False,
                  if_exists='replace',
                  auth_local_webserver=False,
                  table_schema=None,
                  location=None,
                  progress_bar=False,
                  credentials=None,
                  verbose=None,
                  private_key=None)
