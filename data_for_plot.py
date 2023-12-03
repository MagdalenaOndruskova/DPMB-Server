import numpy as np
import pandas as pd

from const import jam_api_url, event_api_url
from models import PlotDataRequestBody
from utils import get_data


def get_data_for_plot(api: str, body: PlotDataRequestBody):
    api = jam_api_url if api == 'jams' else event_api_url
    gdf = get_data(body.from_date_time, body.to_date_time, api)
    resultH = gdf.groupby([pd.Grouper(key='pubMillis', freq='H')]).size().reset_index(name='count')
    resultH['pubMillis_unix'] = resultH['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    data = resultH['count'].values.tolist()
    time_unix = resultH['pubMillis_unix'].values.tolist()
    return data, time_unix


