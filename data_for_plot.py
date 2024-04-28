import numpy as np
import pandas as pd

from const import jam_api_url, event_api_url
from models import PlotDataRequestBody
from utils import get_data, prepare_count_df, get_top_n, filter_df_based_geometry


def get_data_for_plot(api: str, body: PlotDataRequestBody):
    api = jam_api_url if api == 'jams' else event_api_url
    gdf = get_data(body.from_date, body.to_date, api)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    data = resultH['count'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return data, time_unix


def mydiv(a, b):
    try:
        result = a / b
    except ZeroDivisionError:
        result = 0
    return result


def get_data_for_plot_jams(body: PlotDataRequestBody):
    gdf = get_data(body.from_date, body.to_date, jam_api_url)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultHS = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')])[['length', 'level', 'delay', 'speedKMH']].sum().reset_index()

    resultHS = pd.merge(resultH, resultHS, on='pubMillis', how='inner')
    resultHS['delay'] = resultHS['delay'].apply(lambda x: round(x/60, 2))  # to minutes
    resultHS['length'] = resultHS['length'].apply(lambda x: round(x / 1000, 2))  # to km
    resultHS['level'] = resultHS.apply(lambda row: round(mydiv(row['level'], row['count']), 2), axis=1)
    resultHS['speedKMH'] = resultHS.apply(lambda row: round(mydiv(row['speedKMH'], row['count']), 2), axis=1)

    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    length = resultHS['length'].values.tolist()
    level = resultHS['level'].values.tolist()
    delay = resultHS['delay'].values.tolist()
    speedKMH = resultHS['speedKMH'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return length, level, delay, speedKMH, time_unix


def get_data_for_plot_alerts(body: PlotDataRequestBody):
    gdf = get_data(body.from_date, body.to_date, event_api_url)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    data = resultH['count'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return data, time_unix


def get_data_for_plot_bars(body: PlotDataRequestBody):
    gdf_alerts = get_data(body.from_date, body.to_date, event_api_url, "ALERTS", out_streets=body.streets)
    gdf_jams = get_data(body.from_date, body.to_date, jam_api_url, "JAMS", out_streets=body.streets)

    if body.route:
        gdf_jams = filter_df_based_geometry(gdf_jams, body.route)
        gdf_alerts = filter_df_based_geometry(gdf_alerts, body.route)

    gdf_jams = prepare_count_df(gdf_jams)
    streets_jams, values_jams = get_top_n(gdf_jams, n=10)

    gdf_alerts = prepare_count_df(gdf_alerts)
    streets_alerts, values_alerts = get_top_n(gdf_alerts, n=10)

    return streets_jams, values_jams, streets_alerts, values_alerts


def get_data_for_plot_alerts_type(body: PlotDataRequestBody):
    gdf_alerts = get_data(body.from_date, body.to_date, event_api_url, "ALERTS", out_streets=body.streets)

    if body.route:
        gdf_alerts = filter_df_based_geometry(gdf_alerts, body.route)

    gdf_alerts = gdf_alerts[['type', "subtype", "pubMillis"]]



    gdf_basic_types = gdf_alerts.groupby(["type"]).count().reset_index()
    gdf_basic_types = gdf_basic_types.rename(columns={"pubMillis": "count"})
    gdf_basic_types = gdf_basic_types.sort_values(by=['count'], ascending=False)
    basic_types_values = gdf_basic_types['count'].values.tolist()
    basic_types_labels = gdf_basic_types['type'].values.tolist()
    gdf_types = gdf_alerts.groupby(["type", "subtype"]).count().reset_index()
    gdf_types = gdf_types.rename(columns={"pubMillis": "count"})
    gdf_types = gdf_types.replace("", "NOT_DEFINED")
    gdf_types = gdf_types.groupby(['type', 'subtype']).sum().reset_index()
    gdf_types = gdf_types.sort_values(by=['type', 'count'], ascending=False)

    # Group by 'type' and aggregate 'subtype' and 'count' into lists
    grouped_data = gdf_types.groupby('type').agg({
        'subtype': lambda x: x.tolist(),
        'count': lambda x: x.tolist(),
    }).reset_index()

    result = {"basic_types_values": basic_types_values,
              "basic_types_labels": basic_types_labels}

    for _, row in grouped_data.iterrows():
        type_name = row['type']
        subtype_values = row['count']
        subtype_labels = row['subtype']

        result[type_name] = {
            'subtype_values': subtype_values,
            'subtype_labels': subtype_labels,
        }

    return result


def get_data_for_plot_critical_streets_alerts(body: PlotDataRequestBody):
    gdf_alerts = get_data(body.from_date, body.to_date, event_api_url)
    gdf_alerts = gdf_alerts[['street', "pubMillis"]]
    gdf_alerts = gdf_alerts.rename(columns={"pubMillis": "count"})
    gdf_alerts = gdf_alerts.groupby(["street"]).count().reset_index().sort_values(by='count', ascending=False)
    gdf_alerts = gdf_alerts[gdf_alerts['street'].apply(len) >= 2]
    top_10_alerts = gdf_alerts.head(10)
    streets = top_10_alerts['street'].values.tolist()
    values = top_10_alerts['count'].values.tolist()
    return {
        "streets": streets,
        "values": values
    }

