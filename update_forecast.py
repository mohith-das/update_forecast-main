from google.cloud import bigquery
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
from fbprophet import Prophet
from helper import *
import logging
logging.getLogger('fbprophet').setLevel(logging.WARNING)
# import holidays
#
# holiday = pd.DataFrame([])
# for date, name in sorted(holidays.UnitedStates(years=[2020,2021]).items()):
#   holiday = holiday.append(pd.DataFrame({'ds': date, 'holiday': "US-Holidays"}, index=[0]), ignore_index=True)
# holiday['ds'] = pd.to_datetime(holiday['ds'], format='%Y-%m-%d', errors='ignore')


def get_metric_forecast(kpi_df, date_col, metric, interval_width):
    # m = Prophet(interval_width=0.5)
    m = Prophet(interval_width=interval_width)
    df = kpi_df[[date_col, metric]]
    df.columns = ['ds', 'y']
    median = df.loc[((df['y'] - df['y'].mean()) / df['y'].std()).abs() < 3, 'y'].median()
    df.loc[((df['y'] - df['y'].mean()) / df['y'].std()).abs() > 3, 'y'] = np.nan
    df.fillna(median, inplace=True)
    # not_null_mask = df['y'].notnull()
    # min_not_null_index = df[not_null_mask]['y'].index.min()
    # df = df.loc[min_not_null_index:, :]
    df['floor'] = 0
    m.fit(df)

    if date_col == 'DateHour':
        future = m.make_future_dataframe(periods=48, freq='H')
    else:
        future = m.make_future_dataframe(periods=7, freq='D')
        # future = m.make_future_dataframe(periods=2, freq='D')
    future['floor'] = 0
    forecast = m.predict(future)
    forecast = forecast[['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper']]

    # TODO : Review Model and check why floor is not working for organic metrics.
    # Fixing Organic Metrics
    if 'Organic' in metric:
        print(f'Fixing Organic Metric : {metric}')
        for col in ['yhat', 'yhat_lower', 'yhat_upper']:
            min_value = forecast[forecast[col] > 0][col].min()
            forecast[col] = forecast[col].apply(lambda x: x if x > 0 else min_value)

    forecast.columns = forecast.columns.map(lambda x: metric + "_" + x if x != "ds" else date_col)

    return forecast


def get_full_forecast(kpi_df, date_col, dims, metrics):
    cols = kpi_df.columns
    if len(dims) == 0:
        result = pd.DataFrame()
        for metric in metrics:
            interval_width = 0.8 if '__to__' in metric else 0.5
            forecast = get_metric_forecast(kpi_df, date_col, metric, interval_width=interval_width)
            if result.empty:
                result = forecast.copy()
            else:
                result = result.merge(forecast, on=date_col, how='outer')

        total_result = result

    elif len(dims) == 1:
        dims_to_include = get_dims_to_include(kpi_df, date_col, dims, metrics)
        df = kpi_df[kpi_df[dims[0]].isin(dims_to_include)]
        groups = df.groupby(dims[0])
        total_result = pd.DataFrame()
        for index, group_df in groups:
            if group_df.shape[0] <= 10:
                group_df = expand_dates(group_df, date_col)
            result = pd.DataFrame()
            for col in cols:
                if col in metrics:
                    forecast = get_metric_forecast(group_df, date_col, col, interval_width=0.8)
                    if result.empty:
                        result = forecast.copy()
                    else:
                        result = result.merge(forecast, on=date_col, how='outer')

            if len(dims) == 1:
                result[dims[0]] = index
            else:
                for dim, dim_label in zip(dims, index):
                    result[dim] = dim_label
            total_result = pd.concat([total_result, result])

    else:
        raise ValueError(f'length of dims is {len(dims)}')

    return total_result


project_id = 'watchdog-340107'
dataset_id = 'AG_USA'
view_id = 'affiliate_weekly_kpi_view'

# project_id = 'murad-data-warehouse'
# dataset_id = 'watchdog_Murad_US'
# view_id = 'ga_daily_kpi_view'

def update_forecast(project_id, dataset_id, view_id):
    client = bigquery.Client(project=project_id)

    print(dataset_id, view_id)

    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(view_id)
    table = client.get_table(table_ref)

    dims = []
    metrics = []
    possible_date_cols = ['DateHour', 'Date', 'Week', 'Return_Date']
    for schema in table.schema:
        if schema.field_type in ['NUMERIC', 'FLOAT', 'INTEGER']:
            metrics.append(schema.name)
        elif schema.name in possible_date_cols:
            date_col = schema.name
        else:
            dims.append(schema.name)

    if date_col == 'DateHour':
        start_date = date.today() - timedelta(days=30)
    elif date_col == 'Date':
        start_date = date.today() - timedelta(days=30*6)
    else:
        start_date = date.today() - timedelta(days=30*18)
    start_date = start_date.strftime("%Y-%m-%d")

    end_date = date.today() - timedelta(days=1)
    end_date = end_date.strftime("%Y-%m-%d")

    query_string = f"""
    SELECT
    *
    FROM `{project_id}.{dataset_id}.{view_id}` 
    WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY {date_col}
    """

    kpi_df = client.query(query_string).result().to_dataframe()

    if len(kpi_df) == 0:
        print(f'Query Fetched No Result : {query_string}')
        return False

    for metric in metrics:
        kpi_df[metric] = pd.to_numeric(kpi_df[metric])

    total_result = get_full_forecast(kpi_df, date_col, dims, metrics)

    if date_col == 'DateHour':
        total_result[date_col] = total_result[date_col].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        total_result[date_col] = total_result[date_col].dt.strftime('%Y-%m-%d')

    if view_id.endswith('_view'):
        table_id = f'{project_id}.{dataset_id}.{view_id[:-5]}_forecast'
    else:
        table_id = f'{project_id}.{dataset_id}.{view_id}_forecast'

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        total_result, table_id, job_config=job_config
    )

    print(f"Created forecast table {table_id}")

    return job.result()


