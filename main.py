from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette import status

from data_preparation_street import find_square, find_nearest_street
from finding_route import create_graph, find_route_by_streets
from models import RoutingRequestBody
import geopandas as gpd

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
async def get_street(longitude: float, latitude: float):
    coordinates = (float(longitude), float(latitude))
    square_index = find_square(coordinates, grid_gdf)
    streets_in_square = merged_gdf_streets[merged_gdf_streets['grid_squares'].apply(lambda x: str(square_index) in x)]
    nearest_street, path = find_nearest_street(coordinates, streets_in_square, streets_gdf)
    return {"street": nearest_street,
            "path": path}


@app.post("/find_route/")
async def get_route(body: RoutingRequestBody):
    if body.src_street and body.dst_street:
        if body.pass_streets:
            path = []
            streets = []
            indexes = []
            first_street = body.src_street
            # todo merge dictionaries
            for pass_street in body.pass_streets:
                if not pass_street:
                    continue
                path_coordinates, streets_list, indexes_list = find_route_by_streets(first_street, pass_street, street_road_gdf, streets_gdf)
                path += path_coordinates
                streets += streets_list
                first_street = pass_street
            path_coordinates, streets_list, indexes_list = find_route_by_streets(first_street, body.dst_street, street_road_gdf, streets_gdf)
            path += path_coordinates
            streets += streets_list
            indexes += indexes_list
        else:
            path, streets, street_geometry_dict = find_route_by_streets(body.src_street, body.dst_street, street_road_gdf, streets_gdf)
            return {"streets_coord": street_geometry_dict}
        if not path or not streets:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Path not found. Please add some pass streets.',
            )
        streets.append(body.src_street)
        streets.append(body.dst_street)

        # return JSONResponse(content=street_geometry_dict)
        # return {"streets": list(set(streets)),
        #         # "streets_geometry": street_geometry_dict,
        #         "route": path}
    elif body.src_coord and body.dst_coord:
        source_sq_id = find_square(body.src_coord, grid_gdf)
        streets_in_square = merged_gdf_streets[
            merged_gdf_streets['grid_squares'].apply(lambda x: str(source_sq_id) in x)]
        source_street = find_nearest_street(body.src_coord, streets_in_square)

        dst_sq_id = find_square(body.dst_coord, grid_gdf)
        streets_in_square = merged_gdf_streets[
            merged_gdf_streets['grid_squares'].apply(lambda x: str(dst_sq_id) in x)]
        dst_street = find_nearest_street(body.dst_coord, streets_in_square)

        path, streets, indexes = find_route_by_streets(source_street, dst_street, street_road_gdf, streets_gdf)

        if not path or not streets:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Path not found. Please add some pass streets.',
            )
        streets.append(source_street)
        streets.append(dst_street)
        return {"streets": list(set(streets)),
                "route": path,
                }
    else:
        return {"error": "Not enough information for finding a route."}
