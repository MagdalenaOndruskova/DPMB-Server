from datetime import datetime

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette import status

from const import jam_api_url
from data_for_plot import get_data_for_plot
from data_preparation_street import find_square, find_nearest_street, find_color_of_street, get_nearest_street
from finding_route import create_graph, find_route_by_streets, load_graph
from models import RoutingRequestBody, PlotDataRequestBody
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
    nearest_street, path = find_nearest_street(coordinates, streets_in_square, streets_gdf)
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


@app.post("/data_for_plot/")
async def get_route(body: PlotDataRequestBody):
    data_jams, time = get_data_for_plot('jams', body)
    data_alerts, _ = get_data_for_plot('alerts', body)

    return {"jams": data_jams,
            "alerts": data_alerts,
            "xaxis": time}
