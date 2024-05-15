import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from starlette.responses import JSONResponse
from utils import get_street_path, get_final_counts, load_data_from_file, load_data_for_streets, \
    get_paths_for_each_street, get_points_for_drawing_alerts
from apscheduler.schedulers.background import BackgroundScheduler

from data_for_plot import get_data_for_plot_bars, \
    get_data_for_plot_alerts_type, get_data_for_plot_critical_streets_alerts
from data_preparation_street import find_square, find_nearest_street, get_nearest_street
from finding_route import find_route_by_coord, create_graph
from models import  PlotDataRequestBody, RoutingCoordRequestBody, EmailSchema
import geopandas as gpd

import warnings

# dont show warnings in log
warnings.simplefilter(action='ignore', category=FutureWarning)


scheduler = BackgroundScheduler()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

conf = ConnectionConfig(
    MAIL_USERNAME="brno.waze@seznam.cz",
    MAIL_PASSWORD="WazeDataAnalys!s123",
    MAIL_FROM="brno.waze@seznam.cz",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.seznam.cz",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

# loading files needed in different APIs calls
grid_gdf = gpd.read_file("./datasets/streets_grid.geojson")
merged_gdf_streets = gpd.read_file("./datasets/streets_grid_coord.geojson")
streets_gdf = gpd.read_file("./datasets/streets_exploded.geojson")
routing_base = gpd.read_file("./datasets/new_routing_base.geojson")

# creating graph for finding a route
create_graph(routing_base)


def update_data():
    """
    function updates data calculated for whole dataset.
    """
    file_path = "datasets/data_per_day.csv"
    if os.path.exists(file_path):
        # calculate data from last row -3 hours (to correct some theoretical errors)
        df = pd.read_csv(file_path)
        df['pubMillis'] = pd.to_datetime(df['pubMillis'])

        one_year_ago = datetime.now() - timedelta(days=365)
        last_value = df['pubMillis'].iloc[-3]
        df_filtered = df[(df['pubMillis'] >= one_year_ago) & (df['pubMillis'] < last_value + timedelta(hours=1))]
        now_value = (datetime.now() + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')

        counts = get_final_counts(last_value, now_value)
        counts = counts[(counts['pubMillis'] > last_value)]

        counts = pd.concat([df_filtered, counts], ignore_index=True)
        counts.to_csv(file_path, index=False)
    else:
        # file does not exist - create new one
        last_value = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d %H:%M:%S')
        now_value = (datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
        counts = get_final_counts(last_value, now_value)

        counts.to_csv(file_path, index=False)


@app.on_event('startup')
def init_data():
    """
    creates cron job runner, to update data every 5 minutes
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_data, 'cron', minute='*/05')
    scheduler.start()


@app.get("/recount_data/")
async def recount_data():
    os.remove("./datasets/data_per_day.csv")
    print("data removed")
    update_data()

@app.get("/reverse_geocode/street/")
async def get_street(longitude: float, latitude: float, fromTime: str, toTime: str):
    """
    From one coordinate returns whole street to drawn. With calculated delays on it.
    :param longitude: longitude (float type)
    :param latitude: latitude (float type)
    :param fromTime: string format, from what date calculate delays
    :param toTime: string format, to what date calculate delays
    :return: dictionary of street
    """
    coordinates = (float(longitude), float(latitude))
    square_index = find_square(coordinates, grid_gdf)
    streets_in_square = merged_gdf_streets[merged_gdf_streets['grid_squares'].apply(lambda x: str(square_index) in x)]
    nearest_street = find_nearest_street(coordinates, streets_in_square, streets_gdf)
    streets_dict = get_street_path(streets_gdf, nearest_street, fromTime, toTime)
    return {"streets": streets_dict}


@app.get("/street_coord/")
async def get_street_coord(street: str, fromTime: str, toTime: str):
    """
    Function returns to given street its coordinates
    :param street: name of the street
    :param fromTime: string format, from what date calculate delays
    :param toTime: string format, to what date calculate delays
    :return: calculated street
    """
    streets_dict = get_street_path(streets_gdf, street, fromTime, toTime)
    return {"streets": streets_dict}


@app.post("/all_delays/")
async def get_all_delays(body: PlotDataRequestBody):
    """
    function return all delays
    """
    return get_paths_for_each_street(streets_gdf, body.from_date, body.to_date)


@app.post("/find_route_by_coord/")
async def find_route_coord(body: RoutingCoordRequestBody):
    route, streets_dict, src_street, dst_street = find_route_by_coord(body.src_coord, body.dst_coord,
                                                                      body.from_time, body.to_time,
                                                                      streets_gdf, grid_gdf, merged_gdf_streets)
    if not streets_dict:
        return {'streets_coord': []}

    return {"streets_coord": streets_dict,
            "route": list(route.coords),
            "src_street": src_street,
            "dst_street": dst_street}


@app.post("/draw_alerts/")
async def get_points_alerts(body: PlotDataRequestBody):
    points = get_points_for_drawing_alerts(body.from_date, body.to_date, body.streets, body.route)
    return points


@app.post("/data_for_plot_drawer/")
async def get_data_for_plot_drawer(body: PlotDataRequestBody):
    if not body.streets and not body.route:
        data_jams, data_alerts, time, speedKMH, delay, level, length = \
            load_data_from_file(body.from_date, body.to_date)
    else:
        data_jams, data_alerts, time, speedKMH, delay, level, length = \
            load_data_for_streets(body.from_date, body.to_date, body.streets, body.route)

    return {"jams": data_jams,
            "alerts": data_alerts,
            "speedKMH": speedKMH,
            "delay": delay,
            "level": level,
            "length": length,
            "xaxis": time}


@app.post("/data_for_plot_streets/")
async def get_data_for_plot_bar(body: PlotDataRequestBody):
    streets_jams, values_jams, streets_alerts, values_alerts = get_data_for_plot_bars(body)
    return {"streets_jams": streets_jams,
            "values_jams": values_jams,
            "streets_alerts": streets_alerts,
            "values_alerts": values_alerts}


@app.post("/data_for_plot_alerts/")
async def get_data_for_plot_pies(body: PlotDataRequestBody):
    return get_data_for_plot_alerts_type(body)


@app.post("/data_for_plot_critical_streets/")
async def get_data_for_plot_critical_streets(body: PlotDataRequestBody):
    return get_data_for_plot_critical_streets_alerts(body)


@app.post("/send_mail/")
async def send_mail(email: EmailSchema):
    template = f"""
        <html>
        <body>

        <h2>{email.subject}!</h2>
        
        <p>{email.body}</p>
        
        <br><br>
        <p>Kontakt na odosielatela: {email.from_email}</p>
        </body>
        </html>
        """

    message = MessageSchema(
        subject=email.subject,
        recipients=['brno.waze@seznam.cz'],  # List of recipients, as many as you can pass
        body=template,
        subtype="html"
    )

    fm = FastMail(conf)
    await fm.send_message(message)
    print(message)

    return JSONResponse(status_code=200, content={"message": "email has been sent"})


@app.get("/full_data/")
async def get_full_data():
    file_path = "datasets/data_per_day.csv"

    df = pd.read_csv(file_path)
    df['pubMillis'] = pd.to_datetime(df['pubMillis'])
    df['pubMillis_unix'] = df['pubMillis'].astype(np.int64) / int(1e6)  # Convert to Unix timestamp
    pubMillis = df['pubMillis_unix'].tolist()

    count_alerts = df['count_alerts'].tolist()
    count_jams = df['count_jams'].tolist()

    return {
        "jams": count_jams,
        "alerts": count_alerts,
        "xaxis": pubMillis}
