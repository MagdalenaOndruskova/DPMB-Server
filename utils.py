from datetime import timedelta, datetime

import numpy as np
import pandas as pd
import requests
import geopandas as gpd
from shapely import LineString

from const import jam_api_url, event_api_url, FILE_PATH, GREEN_COLOR, ORANGE_COLOR

import warnings

from street_stats import get_stats_on_street

warnings.simplefilter(action='ignore', category=FutureWarning)


def mydiv(a, b):
    try:
        result = a / b
    except ZeroDivisionError:
        result = 0
    return result


def get_data(from_time='2023-11-04', to_time='2023-11-10',
             api_url=jam_api_url, type_api="JAMS", out_fields=None, out_streets=None):
    final_df = None

    date_range = pd.date_range(start=pd.to_datetime(from_time), end=pd.to_datetime(to_time) + timedelta(days=1),
                               freq='1D')
    start_time = pd.to_datetime(from_time)
    end_time = pd.to_datetime(to_time)
    if type_api == "JAMS" and not out_fields:
        out_fields = "pubMillis,level,delay,speedKMH,length,street,blockingAlertUuid"
    if type_api == "ALERTS" and not out_fields:
        out_fields = "pubMillis,subtype,street,type,latitude,longitude"
    for i in range(len(date_range) - 1):
        start_time = date_range[i]
        end_time = date_range[i + 1]

        final_df = get_part_data(api_url, start_time, end_time, final_df, out_fields, out_streets)

    if start_time == pd.to_datetime(from_time) and end_time == pd.to_datetime(to_time):
        final_df = get_part_data(api_url, start_time, end_time, final_df, out_fields, out_streets)
    else:
        final_df = get_part_data(api_url, end_time, pd.to_datetime(to_time), final_df, out_fields, out_streets)

    if final_df is not None:
        return final_df
    return None


def get_part_data(api_url, start_time, end_time, final_df, out_fields="*", out_streets=None):
    query = f"city='Brno' AND pubMillis > TIMESTAMP '{start_time}' AND pubMillis <= TIMESTAMP '{end_time}'"

    if out_streets:
        query_streets = ""
        for street in out_streets:
            street_encoded = reverse_encoding(street)
            if len(query_streets) < 1:
                query_streets = f"street='{street_encoded}'"
            else:
                query_streets = f"{query_streets} OR street='{street_encoded}'"
        query = f"{query} AND ({query_streets})"
    url = f"{api_url}query?where=({query})&outFields={out_fields}&outSR=4326&f=json"
    response = requests.get(url)

    if response.status_code == 200:
        content = response.content.decode('utf-8')
        gdf = gpd.read_file(content)
        gdf['pubMillis'] = pd.to_datetime(gdf['pubMillis'], unit='ms', )
        gdf['street'] = gdf.apply(lambda row: fix_encoding(row['street']), axis=1)
        if final_df is None:
            final_df = gdf
        else:
            final_df = pd.concat([final_df, gdf], ignore_index=True)
    return final_df


def prepare_count_df(df):
    df = df.groupby(['street']).count().reset_index()
    df = df.sort_values(by=['pubMillis'], ascending=False)
    df['street'].replace('', np.nan, inplace=True)
    df = df.dropna(subset=['street'])
    df['count'] = df['pubMillis']
    df = df[['street', 'count']]
    return df


def get_top_n(df, n):
    streets = df['street'].values.tolist()
    values = df['count'].values.tolist()
    streets = streets[:n]
    values = values[:n]
    return streets, values


def fix_encoding(value):
    try:
        fixed = value.replace('Ã¡', 'á')
        fixed = fixed.replace('Ã\xad', 'í')
        fixed = fixed.replace('Åˆ', 'ň')
        fixed = fixed.replace('Ã½', 'ý')
        fixed = fixed.replace('Å™', 'ř')
        fixed = fixed.replace('Å¾', 'ž')
        fixed = fixed.replace('Ä�', 'č')
        fixed = fixed.replace('Å½', 'Ž')
        fixed = fixed.replace('Ã©', 'é')
        fixed = fixed.replace('Ä›', 'ě')
        fixed = fixed.replace('Å¡', 'š')
        fixed = fixed.replace('Å˜', 'Ř')
        fixed = fixed.replace('Å\xa0', 'Š')
        fixed = fixed.replace('ÄŒ', 'Č')
        fixed = fixed.replace('Å¯', 'ů')
        fixed = fixed.replace('Ãš', 'Ú')
        fixed = fixed.replace('Ãº', 'ú')
        fixed = fixed.replace('Ã¼º', 'ü')
        fixed = fixed.replace('Ã¼', 'ü')
        fixed = fixed.replace('Ã¶', 'ö')
        fixed = fixed.replace('Â»', '»')  # ď Ď ä

        return fixed
    except Exception:
        return ''


def reverse_encoding(value):
    try:
        fixed = value.replace('á', 'Ã¡')
        fixed = fixed.replace('í', 'Ã\xad')
        fixed = fixed.replace('ň', 'Åˆ')
        fixed = fixed.replace('ý', 'Ã½')
        fixed = fixed.replace('ř', 'Å™')
        fixed = fixed.replace('ž', 'Å¾')
        fixed = fixed.replace('č', 'Ä�')
        fixed = fixed.replace('Ž', 'Å½')
        fixed = fixed.replace('é', 'Ã©')
        fixed = fixed.replace('ě', 'Ä›')
        fixed = fixed.replace('š', 'Å¡')
        fixed = fixed.replace('Ř', 'Å˜')
        fixed = fixed.replace('Š', 'Å\xa0')
        fixed = fixed.replace('Č', 'ÄŒ')
        fixed = fixed.replace('ů', 'Å¯')
        fixed = fixed.replace('Ú', 'Ãš')
        fixed = fixed.replace('ú', 'Ãº')
        fixed = fixed.replace('ü', 'Ã¼º')
        fixed = fixed.replace('ü', 'Ã¼')
        fixed = fixed.replace('ö', 'Ã¶')
        fixed = fixed.replace('»', 'Â»')
        return fixed
    except Exception:
        return ''


def assign_color(df, num_days=7):
    df['color'] = np.select(
        [
            df['count'] < GREEN_COLOR * num_days,
            (df['count'] >= GREEN_COLOR * num_days) & (df['count'] <= ORANGE_COLOR * num_days),
            df['count'] > ORANGE_COLOR
        ],
        ['green', 'orange', 'red'],
    )
    return df


def get_color(df, street_name, column_name):
    if street_name in df[column_name].values:
        color = df.loc[df[column_name] == street_name, 'color'].values[0]
        return color
    else:
        return 'green'


def count_delays_by_parts(gdf, data):
    for index, row in gdf.iterrows():
        street_name = row['nazev']
        geometry = row['geometry']
        intersection_count = 0

        # Filter data GeoDataFrame based on street_name
        filtered_data = data[data['street'] == street_name]

        # Iterate over each row in filtered_data
        for _, data_row in filtered_data.iterrows():
            data_geometry = data_row['geometry']

            # Check if the geometries intersect
            if geometry.intersects(data_geometry):
                intersection_count += 1

        # Update intersection count for the current part
        gdf.at[index, 'count'] = intersection_count
    df_count = assign_color(gdf)

    return df_count


def get_street_path(gdf, street, fromTime, toTime, data=gpd.GeoDataFrame()):
    df_streets = gdf[gdf['nazev'] == street] if street else gdf
    data = get_data(fromTime, toTime, out_streets=[street]) if len(data) < 1 else data
    df_count = count_delays_by_parts(df_streets, data)

    street_geometry_dict = []

    for index, row in df_count.iterrows():
        final_dict = {'street_name': row['nazev'],
                      'path': [[long, lat] for lat, long in row['geometry'].coords],
                      'color': row['color']}
        street_geometry_dict += [final_dict]
    return street_geometry_dict


def get_paths_for_each_street(gdf, fromTime, toTime):
    streets_data = []
    unique_streets = gdf['nazev'].unique()
    gdf_data = get_data(fromTime, toTime, out_fields="pubMillis,street")
    return get_street_path(gdf, None, None, None, gdf_data)


def load_data_for_streets(from_date, to_date, streets, route):
    if not streets:
        streets = []
    final_df = get_data(from_time=from_date, to_time=to_date,
                        api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/",
                        type_api="JAMS", out_streets=streets)
    final_df_alerts = get_data(from_time=from_date, to_time=to_date,
                               api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeAlerts/FeatureServer/0/",
                               type_api="ALERTS", out_streets=streets)

    final_df['pubMillis'] = pd.to_datetime(final_df['pubMillis'])
    final_df_alerts['pubMillis'] = pd.to_datetime(final_df_alerts['pubMillis'])
    if route:
        final_df = filter_df_based_geometry(final_df, route)
        final_df_alerts = filter_df_based_geometry(final_df_alerts, route)

    count_per_day_hs = final_df.groupby([pd.Grouper(key='pubMillis', freq='H')])[
        ['length', 'level', 'delay', 'speedKMH']].sum().reset_index()
    final_df = final_df.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index(name='count_jams')

    resultHS = pd.merge(final_df, count_per_day_hs, on='pubMillis', how='inner')
    resultHS['delay'] = resultHS['delay'].apply(lambda x: round(x / 60, 2))  # to minutes
    resultHS['length'] = resultHS['length'].apply(lambda x: round(x / 1000, 2))  # to km
    resultHS['level'] = resultHS.apply(lambda row: round(mydiv(row['level'], row['count_jams']), 2), axis=1)
    resultHS['speedKMH'] = resultHS.apply(lambda row: round(mydiv(row['speedKMH'], row['count_jams']), 2), axis=1)

    final_df_alerts = final_df_alerts.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index(
        name='count_alerts')

    resultHS['pubMillis_unix'] = resultHS['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    final_df_alerts['pubMillis_unix'] = final_df_alerts['pubMillis'].astype(np.int64) / int(
        1e6)  # Convert to Unix timestamp
    pubMillis = resultHS['pubMillis_unix'].tolist()

    count_alerts = final_df_alerts['count_alerts'].tolist()
    count_jams = resultHS['count_jams'].tolist()

    length = resultHS['length'].tolist()
    level = resultHS['level'].tolist()
    delay = resultHS['delay'].tolist()
    speedKMH = resultHS['speedKMH'].tolist()
    return count_jams, count_alerts, pubMillis, speedKMH, delay, level, length


def load_data_from_file(from_date, to_date):
    df = pd.read_csv(FILE_PATH)
    df['pubMillis'] = pd.to_datetime(df['pubMillis'])
    df['pubMillis_unix'] = df['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    df_filtered = df[(df['pubMillis'] >= pd.to_datetime(from_date)) &
                     (df['pubMillis'] <= pd.to_datetime(to_date) + timedelta(days=1))]
    pubMillis = df_filtered['pubMillis_unix'].tolist()

    count_alerts = df_filtered['count_alerts'].tolist()
    count_jams = df_filtered['count_jams'].tolist()
    length = df_filtered['length'].tolist()
    level = df_filtered['level'].tolist()
    delay = df_filtered['delay'].tolist()
    speedKMH = df_filtered['speedKMH'].tolist()
    return count_jams, count_alerts, pubMillis, speedKMH, delay, level, length


def get_final_counts(from_time, to_time):
    final_df = get_data(from_time=from_time, to_time=to_time,
                        api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/")
    final_df_alerts = get_data(from_time=from_time, to_time=to_time,
                               api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeAlerts/FeatureServer/0/",
                               type_api="ALERTS")
    count_per_day = final_df.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index(name='count_jams')
    count_per_day_hs = final_df.groupby([pd.Grouper(key='pubMillis', freq='H')])[
        ['length', 'level', 'delay', 'speedKMH']].sum().reset_index()

    resultHS = pd.merge(count_per_day, count_per_day_hs, on='pubMillis', how='inner')
    resultHS['delay'] = resultHS['delay'].apply(lambda x: round(x / 60, 2))  # to minutes
    resultHS['length'] = resultHS['length'].apply(lambda x: round(x / 1000, 2))  # to km
    resultHS['level'] = resultHS.apply(lambda row: round(mydiv(row['level'], row['count_jams']), 2), axis=1)
    resultHS['speedKMH'] = resultHS.apply(lambda row: round(mydiv(row['speedKMH'], row['count_jams']), 2), axis=1)

    count_per_day_alerts = final_df_alerts.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index()
    count_per_day_alerts = count_per_day_alerts.rename(columns={0: 'count_alerts'})

    counts = pd.merge(resultHS, count_per_day_alerts, on='pubMillis', how="outer")
    counts = counts.fillna(0)
    counts = counts.astype({'count_alerts': 'int64'})
    counts = counts.astype({'count_jams': 'int64'})

    return counts


def count_days_between_dates(date_str1, date_str2, date_format="%Y-%m-%d"):
    try:
        date1 = datetime.strptime(date_str1, date_format)
        date2 = datetime.strptime(date_str2, date_format)
        delta = abs(date2 - date1)
        return delta.days + 1
    except ValueError:
        return "Invalid date format"


def find_color_of_street(from_time, to_time, street, gdf):
    num_days = count_days_between_dates(from_time, to_time)
    df_count = get_stats_on_street(gdf, street)
    df_count = assign_color(df_count, num_days)
    return get_color(df_count, street, 'street')


def filter_df_based_geometry(gdf, route):
    line = LineString(route).buffer(0.00001)

    for index, row in gdf.iterrows():
        if line.intersects(row['geometry']):
            gdf.at[index, 'in_route'] = True
        else:
            gdf.at[index, 'in_route'] = False

    intersecting_gdf = gdf[gdf['in_route']]
    return intersecting_gdf


def get_points_for_drawing_alerts(from_time, to_time, streets, route):
    if streets:
        gdf = get_data(from_time, to_time, api_url=event_api_url,
                       out_fields="pubMillis,type,subtype,street,longitude,latitude,uuid", out_streets=streets)
    else:
        gdf = get_data(from_time, to_time, api_url=event_api_url,
                       out_fields="pubMillis,type,subtype,street,longitude,latitude,uuid")

    gdf['key'] = gdf['uuid']
    gdf = gdf[['street', "pubMillis", "type", "subtype", "longitude", "latitude", "key", "geometry"]].reset_index()

    if route:
        gdf = filter_df_based_geometry(gdf, route)
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)

    return gdf.to_dict(orient='records')
