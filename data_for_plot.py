import numpy as np
import pandas as pd

from const import jam_api_url, event_api_url
from models import PlotDataRequestBody
from utils import get_data, prepare_count_df, get_top_n


def get_data_for_plot(api: str, body: PlotDataRequestBody):
    api = jam_api_url if api == 'jams' else event_api_url
    gdf = get_data(body.from_date_time, body.to_date_time, api)
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
    gdf = get_data(body.from_date_time, body.to_date_time, jam_api_url)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultHS = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')])[['length', 'level', 'delay', 'speedKMH']].sum().reset_index()
    resultHS = pd.merge(resultH, resultHS, on='pubMillis', how='inner')
    resultHS['delay'] = resultHS['delay'].apply(lambda x: round(x/60, 2))  # to minutes
    resultHS['length'] = resultHS['length'].apply(lambda x: round(x / 1000, 2))  # to km
    resultHS['level'] = resultHS.apply(lambda row: round(mydiv(row['level'], row['count']), 2), axis=1)
    resultHS['speedKMH'] = resultHS.apply(lambda row: round(mydiv(row['speedKMH'], row['count']), 2), axis=1)

    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    data = resultH['count'].values.tolist()
    length = resultHS['length'].values.tolist()
    level = resultHS['level'].values.tolist()
    delay = resultHS['delay'].values.tolist()
    speedKMH = resultHS['speedKMH'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return data, length, level, delay, speedKMH, time_unix


def get_data_for_plot_alerts(body: PlotDataRequestBody):
    gdf = get_data(body.from_date_time, body.to_date_time, event_api_url)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    data = resultH['count'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return data, time_unix


def get_data_for_plot_bars(body: PlotDataRequestBody):
    gdf_alerts = get_data(body.from_date_time, body.to_date_time, event_api_url)
    gdf_jams = get_data(body.from_date_time, body.to_date_time, jam_api_url)

    gdf_jams = prepare_count_df(gdf_jams)
    streets_jams, values_jams = get_top_n(gdf_jams, n=10)

    gdf_alerts = prepare_count_df(gdf_alerts)
    streets_alerts, values_alerts = get_top_n(gdf_alerts, n=10)

    return streets_jams, values_jams, streets_alerts, values_alerts


def get_data_for_plot_alerts_type(body: PlotDataRequestBody):
    gdf_alerts = get_data(body.from_date_time, body.to_date_time, event_api_url)
    gdf_alerts = gdf_alerts[['type', "subtype", "pubMillis"]]
    gdf_basic_types = gdf_alerts.groupby(["type"]).count().reset_index()
    gdf_basic_types = gdf_basic_types.rename(columns={"pubMillis": "count"})
    gdf_basic_types = gdf_basic_types.sort_values(by='type')
    basic_types_values = gdf_basic_types['count'].values.tolist()
    basic_types_labels = gdf_basic_types['type'].values.tolist()
    gdf_types = gdf_alerts.groupby(["type", "subtype"]).count().reset_index()
    gdf_types = gdf_types.rename(columns={"pubMillis": "count"})
    gdf_types = gdf_types.replace("", "NOT_DEFINED")

    # Identify types with more than 4 subtypes
    types_with_more_than_4_subtypes = gdf_types['type'].value_counts()[gdf_types['type'].value_counts() > 4].index

    # Create an empty DataFrame to store IQR information
    iqr_df = pd.DataFrame(columns=['type', 'iqr'])

    # Iterate over types with more than 4 subtypes and calculate IQR
    for type_name in types_with_more_than_4_subtypes:
        type_data = gdf_types[gdf_types['type'] == type_name]
        iqr = type_data['count'].quantile([0.25, 0.75]).diff().iloc[-1]
        iqr_df = pd.concat([iqr_df, pd.DataFrame({'type': [type_name], 'iqr': [iqr]})])

    # Merge the IQR information back to the original DataFrame
    gdf_types = pd.merge(gdf_types, iqr_df, on='type', how='left')

    # Create a function to check if a count is less than the IQR for a given type
    def is_below_iqr(row):
        if pd.notna(row['iqr']):
            return row['count'] < row['iqr']
        return False

    # Apply the function to create a new subtype 'others' for counts below IQR
    gdf_types['subtype'] = np.where((gdf_types['subtype'] != 'NOT_DEFINED') & gdf_types.apply(is_below_iqr, axis=1),
                                    'OTHERS', gdf_types['subtype'])
    gdf_types = gdf_types.groupby(['type', 'subtype']).sum().reset_index()
    gdf_types = gdf_types.sort_values(by='subtype')

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

