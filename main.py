from datetime import datetime

import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from starlette import status
from starlette.responses import JSONResponse

from const import jam_api_url
from data_for_plot import get_data_for_plot, get_data_for_plot_jams, get_data_for_plot_alerts, get_data_for_plot_bars, \
    get_data_for_plot_alerts_type, get_data_for_plot_critical_streets_alerts
from data_preparation_street import find_square, find_nearest_street, find_color_of_street, get_nearest_street
from finding_route import create_graph, find_route_by_streets, load_graph, find_nearest_point, find_route_by_coord
from models import RoutingRequestBody, PlotDataRequestBody, RoutingCoordRequestBody, EmailSchema
import geopandas as gpd

from street_stats import prepare_stats_count
from utils import get_data, get_street_path

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
street_road_gdf = gpd.read_file("./datasets/streets_road_data.geojson")
streets_gdf = gpd.read_file("./datasets/streets_exploded.geojson")

# creating graph for finding a route
create_graph(street_road_gdf)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/reverse_geocode/street/")
async def get_street(longitude: float, latitude: float, fromTime: str, toTime: str):
    coordinates = (float(longitude), float(latitude))
    square_index = find_square(coordinates, grid_gdf)
    streets_in_square = merged_gdf_streets[merged_gdf_streets['grid_squares'].apply(lambda x: str(square_index) in x)]
    nearest_street = find_nearest_street(coordinates, streets_in_square, streets_gdf)
    path = get_street_path(streets_gdf, nearest_street)
    color = find_color_of_street(fromTime, toTime, nearest_street)
    return {"street": nearest_street,
            "path": path,
            "color": color}


@app.get("/street_coord/")
async def get_street_coord(street: str, fromTime: str, toTime: str):
    path = get_street_path(streets_gdf, street)
    color = find_color_of_street(fromTime, toTime, street)
    return {"path": path,
            "color": color,
            "street": street}


@app.post("/find_route/")
async def get_route(body: RoutingRequestBody):
    if body.src_street and body.dst_street:
        if body.pass_streets:
            path = []
            streets = []
            streets_dict = []
            first_street = body.src_street
            for pass_street in body.pass_streets:
                if not pass_street:
                    continue
                path_coordinates, streets_list, streets_geometry_dict = \
                    find_route_by_streets(first_street, pass_street, body.from_time, body.to_time,
                                          street_road_gdf, streets_gdf)
                path += path_coordinates
                streets += streets_list
                streets_dict += streets_geometry_dict
                first_street = pass_street
            path_coordinates, streets_list, streets_geometry_dict = find_route_by_streets(first_street, body.dst_street,
                                                                                          body.from_time, body.to_time,
                                                                                          street_road_gdf, streets_gdf)
            path += path_coordinates
            streets += streets_list
            streets_dict += streets_geometry_dict
            # todo: throw out the routes not relevant to path? but what about the count? should count happen after this?
        else:
            _, _, streets_dict = find_route_by_streets(body.src_street, body.dst_street, body.from_time, body.to_time,
                                                       street_road_gdf, streets_gdf)
        if not streets_dict:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Path not found. Please add some pass streets.',
            )
        return {"streets_coord": streets_dict}

    elif body.src_coord and body.dst_coord:
        source_street = get_nearest_street(body.src_coord, grid_gdf, merged_gdf_streets)
        dst_street = get_nearest_street(body.dst_coord, grid_gdf, merged_gdf_streets)

        path, streets, streets_dict = find_route_by_streets(source_street, dst_street, street_road_gdf, streets_gdf)

        if not streets_dict:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Path not found. Please add some pass streets.',
            )
        return {"streets_coord": streets_dict}
    else:
        return {"error": "Not enough information for finding a route."}


@app.post("/find_route_by_coord/")
async def find_route_coord(body: RoutingCoordRequestBody):
    src_in_graph = find_nearest_point(body.src_coord)
    dst_in_graph = find_nearest_point(body.dst_coord)

    path, streets, streets_dict = find_route_by_coord(src_in_graph, dst_in_graph, body.from_time, body.to_time,
                                                      streets_gdf, grid_gdf, merged_gdf_streets)
    if not streets_dict:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Path not found. Please add some pass streets.',
        )
    return {"streets_coord": streets_dict}


@app.post("/data_for_plot/")
async def get_data_for_plot(body: PlotDataRequestBody):
    data_jams, length, level, delay, speedKMH, time = get_data_for_plot_jams(body)
    data_alerts, _ = get_data_for_plot_alerts(body)

    return {"jams": data_jams,
            "length": length,
            "level": level,
            "delay": delay,
            "speedKMH": speedKMH,
            "alerts": data_alerts,
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
