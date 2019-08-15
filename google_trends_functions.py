import datetime
import random
import pandas as pd
import tqdm
from dateutil.relativedelta import relativedelta


def prev_week(date):
    idx = (date.weekday() + 2) % 7
    last_week_end = date - datetime.timedelta(idx)
    last_week_start = last_week_end - datetime.timedelta(days=+6)
    d1 = last_week_start.strftime("%Y-%m-%d")
    d2 = last_week_end.strftime("%Y-%m-%d")
    time = d1 + ' ' + d2
    return time


def date_points(period):
    start = (datetime.date.today() - datetime.timedelta(days=+1))
    idx = (start.weekday() + 2) % 7
    last_week_end = start - datetime.timedelta(idx)

    if period == 'week':
        period_end = last_week_end
        period_start = last_week_end - datetime.timedelta(days=+6)

    elif period == 'month':
        period_end = last_week_end
        period_start = (last_week_end - relativedelta(months=+1))

    elif period == 'quarter':
        period_end = last_week_end
        period_start = (last_week_end - relativedelta(months=+3))

    elif period == 'half':
        period_end = last_week_end
        period_start = (last_week_end - relativedelta(months=+6))

    elif period == 'year':
        period_end = last_week_end
        period_start = (last_week_end - relativedelta(years=+1))

    else:
        raise ValueError("Invalid input: '" + period +
                         "', please try one of 'week',\
                          'month', 'quarter', 'half', 'year'")

        return float('nan'), float('nan')

    d1 = period_start.strftime("%Y-%m-%d")
    d2 = period_end.strftime("%Y-%m-%d")

    return d1, d2


def col_trim(string, df_column, reg=True):
    return df_column.str.replace(string, "", regex=reg)


def refactor_data(kw_dic, excl_dic, t, start, df, Final_df, period=''):

    for key, val in kw_dic.items():

        for v in val:

            dataframe = df[v]['top']

            if dataframe is None:
                dataframe = pd.DataFrame(columns=['query', 'value'])
                dataframe.loc[0] = ['Not Enough Data', 100]

            else:
                pass

            for ex in excl_dic[key]:
                dataframe = dataframe[~dataframe['query'].str.contains(ex)]

            dataframe['query'] = col_trim(key, dataframe['query'], reg=False)

            regex_list = [r"\suk$", r"\scast$", r"\s20\d{2}$",
                          r"\sthe$", r"\sthe$", r"\s{2}"]

            for r in regex_list:
                dataframe['query'] = col_trim(r, dataframe['query'])

            dataframe['query'] = dataframe['query'].str.strip()

            dataframe = dataframe.groupby('query').sum()
            dataframe = dataframe.sort_values('value', ascending=False)

            new_index = dataframe['value'].max()
            dataframe['value'] = (dataframe['value'] / new_index) * 100
            dataframe['value'] = dataframe['value'].apply(int)

            # When we did our 'groupby' aggregation earlier, it actually puts
            # our defined column as the DF index so we need to reset this
            dataframe = dataframe.reset_index()

            # We want a couple of columns to tell us what time period
            # we're looking at
            dataframe['date_from'] = t.split()[0]
            dataframe['date_to'] = t.split()[1]

            # It would also be handy to have a nice rank
            dataframe['rank'] = dataframe.index + 1

            # Let's format the Query column as well so it looks a bit prettier
            dataframe['query'] = dataframe['query'].str.title()

            # Create a column for the day the query was run
            dataframe['day_run'] = start.strftime("%Y-%m-%d")

            # Create a column for the day the query was run
            dataframe['key_word'] = key.title()

            dataframe['period'] = period

            Final_df = Final_df.append(dataframe)

    return Final_df


def kw_prep(kw, excl_dic):

    kw_dic = {}

    for k in kw:
        kw_dic[k] = []

    # For each key word we want to search:
    for query in kw:

        # Open up the dictionary of exclusion queries:
        for k, v in excl_dic.items():

            # If the keyword and dictionary key match:
            if query == k:

                # Tell me how long the dictionary is
                excl_len = len(v)

                # If the list of exclusions is > 12 the API call won't work as
                # Google kicks off, so we need to split up

                if excl_len > 12:

                    # Split up our list into n lists, by splitting at each 12th
                    # exclusion
                    composite_excl_list = [v[x:x + 11] for x in range(0,
                                                                      len(v),
                                                                      11)]

                    # The final list won't have a full list of exclusions,
                    # which could return dodgy results so we pad out with
                    # previously used ones
                    missing_vals = 11 - len(composite_excl_list[-1])

                    rand_smpl = random.sample(composite_excl_list[0],
                                              k=missing_vals)

                    composite_excl_list[-1] =\
                        composite_excl_list[-1] + rand_smpl

                    for l in composite_excl_list:
                        search = k
                        for i in l:
                            search = search + ' -' + i

                        kw_dic[k].append(search)

                else:
                    search = k
                    for i in v:
                        search = search + ' -' + i

                    kw_dic[k].append(search)

    kw_list = []

    for key, val in kw_dic.items():
        for item in val:
            kw_list.append(item)

    return kw_dic, kw_list


def agg_table_refactor(input_df, kw_dic, final_df):

    for kw in kw_dic.keys():
        kw_df = input_df[input_df['key_word'] == kw.title()]

        grouped_df = kw_df[['query', 'key_word', 'date_from', 'date_to',
                            'value', 'period']].groupby(['query',
                                                         'key_word',
                                                         'date_from',
                                                         'date_to',
                                                         'period']).sum()

        sorted_df = grouped_df.sort_values('value', ascending=False)

        new_index = sorted_df['value'].max()
        sorted_df['value'] = (sorted_df['value'] / new_index) * 100
        sorted_df['value'] = sorted_df['value'].apply(int)

        remove_name_index_df = sorted_df.reset_index()

        remove_name_index_df['rank'] = remove_name_index_df.index + 1

        final_df = final_df.append(remove_name_index_df)

    return final_df


def iot_periods(period_length, iteration_length, num_days, pytrends, kw_list,
                kw_dic, kw_exclusions):

    d1, d2 = date_points(period_length)

    start = datetime.datetime.strptime(d1, "%Y-%m-%d")
    stop = datetime.datetime.strptime(d2, "%Y-%m-%d")

    date_list = []

    while start <= stop:

        if iteration_length.title() == 'Daily':
            date_list.append(start.strftime("%Y-%m-%d"))
            start = start + datetime.timedelta(days=1)

        elif iteration_length.title() == 'Weekly':
            if start.weekday() == 6 and\
               start + datetime.timedelta(days=6) <= stop:

                date_list.append(start.strftime("%Y-%m-%d"))
            else:
                pass
            start = start + datetime.timedelta(days=1)

        else:
            ValueError("Invalid input: '" + iteration_length + "',\
                        please try either 'Daily'/'Weekly'")

            return pd.DataFrame()

    Final_df = pd.DataFrame()

    # Iterates through each backfill date

    for dt in tqdm.tqdm_notebook(date_list):

        start = datetime.datetime.strptime(dt, "%Y-%m-%d")
        end = start + datetime.timedelta(days=num_days)

        start_window = start.strftime("%Y-%m-%d")
        end_window = end.strftime("%Y-%m-%d")

        # Define our date_window of interest - the last full week
        t = start_window + ' ' + end_window

        # Build pytrends payload - returns an object
        pytrends.build_payload(kw_list, cat=0, timeframe=t, geo='GB', gprop='')

        # Call method of interest on our pytrends payload object
        df = pytrends.related_queries()

        Final_df = refactor_data(kw_dic, kw_exclusions, t, start, df, Final_df,
                                 period=iteration_length)

    return Final_df
