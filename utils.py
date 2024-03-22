from datetime import timedelta

import numpy as np
import pandas as pd
import requests
import geopandas as gpd

from const import jam_api_url, FILE_PATH

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
             api_url=jam_api_url, type_api="JAMS"):
    final_df = None

    date_range = pd.date_range(start=pd.to_datetime(from_time), end=pd.to_datetime(to_time) + timedelta(days=1), freq='1D')
    start_time = pd.to_datetime(from_time)
    end_time = pd.to_datetime(to_time)
    if type_api == "JAMS":
        out_fields = "pubMillis,level,delay,speedKMH,length,street,blockingAlertUuid"
    else:
        out_fields ="pubMillis,subtype,street,type,latitude,longitude"
    for i in range(len(date_range) - 1):
        start_time = date_range[i]
        end_time = date_range[i + 1]

        final_df = get_part_data(api_url, start_time, end_time, final_df, out_fields)

    if start_time == pd.to_datetime(from_time) and end_time == pd.to_datetime(to_time):
        final_df = get_part_data(api_url, start_time, end_time, final_df, out_fields)
    else:
        final_df = get_part_data(api_url, end_time, pd.to_datetime(to_time), final_df, out_fields)

    if final_df is not None:
        return final_df
    return None


def get_part_data(api_url, start_time, end_time, final_df, out_fields="*"):
    #https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/query?where=1%3D1&outFields=uuid,pubMillis,level,delay,speedKMH,length,street,blockingAlertUuid&outSR=4326&f=json
    query = f"city='Brno' AND pubMillis > TIMESTAMP '{start_time}' AND pubMillis <= TIMESTAMP '{end_time}'"

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
        fixed = fixed.replace('Â»', '»') #ď Ď ä

        return fixed
    except Exception:
        return ''


def assign_color(df):
    df['color'] = np.select(
        [
            df['count'] < 50,
            (df['count'] >= 50) & (df['count'] <= 100),
            df['count'] > 100
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


def get_street_path(gdf, street, fromTime, toTime):
    df_streets = gdf[gdf['nazev'] == street]
    path = None
    for index, row in df_streets.iterrows():
        geometry = row['geometry']
        coordinates = geometry.coords
        coordinates2 = [[long, lat] for lat, long in coordinates]
        if not path:
            path = [coordinates2]
        else:
            path = path + [coordinates2]
    color = find_color_of_street(fromTime, toTime, street)

    return path, color


def get_paths_for_each_street(gdf, fromTime, toTime):
    streets_data = []
    unique_streets = gdf['nazev'].unique()
    gdf_data = get_data(fromTime, toTime)

    for street in unique_streets:
        df_streets = gdf[gdf['nazev'] == street]
        path = []
        for index, row in df_streets.iterrows():
            geometry = row['geometry']
            coordinates = geometry.coords
            coordinates2 = [[long, lat] for lat, long in coordinates]
            path.append(coordinates2)
        color = find_color_of_street(fromTime, toTime, street, gdf_data)
        street_data = {'street': street, 'path': path, 'color': color}
        streets_data.append(street_data)

    return streets_data

def load_data_for_streets(from_date, to_date, streets):
        final_df = get_data(from_time=from_date, to_time=to_date,
                            api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/",
                            type_api="JAMS")
        final_df_alerts = get_data(from_time=from_date, to_time=to_date,
                                   api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeAlerts/FeatureServer/0/",
                                   type_api="ALERTS")

        final_df['pubMillis'] = pd.to_datetime(final_df['pubMillis'])
        final_df_alerts['pubMillis'] = pd.to_datetime(final_df_alerts['pubMillis'])

        if streets:
            final_df = final_df[final_df['street'].isin(streets)]
            final_df_alerts = final_df_alerts[final_df_alerts['street'].isin(streets)]

        count_per_day_hs = final_df.groupby([pd.Grouper(key='pubMillis', freq='H')])[
            ['length', 'level', 'delay', 'speedKMH']].sum().reset_index()
        final_df = final_df.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index(name='count_jams')

        resultHS = pd.merge(final_df, count_per_day_hs, on='pubMillis', how='inner')
        resultHS['delay'] = resultHS['delay'].apply(lambda x: round(x / 60, 2))  # to minutes
        resultHS['length'] = resultHS['length'].apply(lambda x: round(x / 1000, 2))  # to km
        resultHS['level'] = resultHS.apply(lambda row: round(mydiv(row['level'], row['count_jams']), 2), axis=1)
        resultHS['speedKMH'] = resultHS.apply(lambda row: round(mydiv(row['speedKMH'], row['count_jams']), 2), axis=1)

        final_df_alerts = final_df_alerts.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index( name='count_alerts')

        resultHS['pubMillis_unix'] = resultHS['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
        final_df_alerts['pubMillis_unix'] = final_df_alerts['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
        pubMillis = final_df_alerts['pubMillis_unix'].tolist()

        count_alerts = final_df_alerts['count_alerts'].tolist()
        count_jams = resultHS['count_jams'].tolist()

        length = resultHS['length'].tolist()
        level = resultHS['level'].tolist()
        delay = resultHS['delay'].tolist()
        speedKMH = resultHS['speedKMH'].tolist()
        return count_jams, count_alerts, pubMillis,  speedKMH, delay, level, length


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
    return count_jams, count_alerts, pubMillis,  speedKMH, delay, level, length


def get_final_counts(from_time, to_time):
    final_df = get_data(from_time=from_time, to_time=to_time,
                        api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/")
    final_df_alerts = get_data(from_time=from_time, to_time=to_time,
                               api_url="https://gis.brno.cz/ags1/rest/services/Hosted/WazeAlerts/FeatureServer/0/", type_api="ALERTS")
    count_per_day = final_df.groupby(pd.Grouper(key='pubMillis', freq='1H')).size().reset_index(name='count_jams')
    count_per_day_hs = final_df.groupby([pd.Grouper(key='pubMillis', freq='H')])[['length', 'level', 'delay', 'speedKMH']].sum().reset_index()

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


def find_color_of_street(from_time, to_time, street, gdf):
    df_count = get_stats_on_street(gdf, street)
    df_count = assign_color(df_count)
    return get_color(df_count, street, 'street')