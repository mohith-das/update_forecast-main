import pandas as pd
from datetime import datetime, date, timedelta


yesterday = date.today() - timedelta(days=2)
yesterday = yesterday.strftime("%Y-%m-%d")
prev_period = date.today() - timedelta(days=30)
prev_period = prev_period.strftime("%Y-%m-%d")


def expand_dates(group_df, date_col):


    group_df[date_col] = pd.to_datetime(group_df[date_col])
    group_df.set_index(date_col, inplace=True)
    idx = pd.date_range(start=prev_period, end=yesterday, name=date_col)
    group_df = group_df.reindex(index=idx, fill_value=0)
    group_df.reset_index(inplace=True)
    return group_df


def get_dims_to_include(kpi_df, date_col, dims, metrics):
    TOP_N = 5

    all_dim_labels = kpi_df[dims[0]].unique()
    if len(all_dim_labels) <= TOP_N:
        return all_dim_labels

    if 'Revenue' in metrics:
        metric = 'Revenue'
    elif 'Total_Sales' in metrics:
        metric = 'Total_Sales'
    elif 'Spend' in metrics:
        metric = 'Spend'
    elif 'Clicks' in metrics:
        metric = 'Clicks'
    elif 'Sessions' in metrics:
        metric = 'Sessions'
    elif 'Organic_Sales' in metrics:
        metric = 'Organic_Sales'
    elif 'Quantity' in metrics:
        metric = 'Quantity'
    elif 'Product_Checkouts' in metrics:
        metric = 'Product_Checkouts'
    elif 'Product_Detail_Views' in metrics:
        metric = 'Product_Detail_Views'
    else:
        raise ValueError(f"No valid metric to calculate top {TOP_N} dim_labels")
    yesterday_mask = kpi_df[date_col] == yesterday
    yesterday_df = kpi_df[yesterday_mask].groupby(dims[0])[[metric]].sum()
    yesterday_df['Share'] = yesterday_df[metric] / yesterday_df[metric].sum()
    yesterday_share_mask = yesterday_df['Share'] > 0.05
    yesterday_share_df = yesterday_df[yesterday_share_mask]
    yesterday_dim_labels = yesterday_share_df[metric].nlargest(TOP_N).index

    if len(yesterday_dim_labels) < TOP_N:
        yesterday_dim_labels = yesterday_df[metric].nlargest(TOP_N).index

    prev_period_mask = kpi_df[date_col] >= prev_period
    prev_period_df = kpi_df[prev_period_mask].groupby(dims[0])[[metric]].sum()
    prev_period_df['Share'] = prev_period_df[metric] / prev_period_df[metric].sum()
    prev_period_share_mask = prev_period_df['Share'] > 0.05
    prev_period_share_df = prev_period_df[prev_period_share_mask]
    prev_period_dim_labels = prev_period_share_df[metric].nlargest(TOP_N).index

    if len(prev_period_dim_labels) < TOP_N:
        prev_period_dim_labels = prev_period_df[metric].nlargest(TOP_N).index

    dims_to_include = list(set(yesterday_dim_labels) | set(prev_period_dim_labels))
    return dims_to_include
