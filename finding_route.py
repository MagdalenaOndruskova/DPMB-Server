from typing import Tuple

from math import radians, cos, sin, sqrt, atan2

import networkx as nx
from geopandas import GeoDataFrame
from shapely import LineString, MultiLineString, Point, intersection
from geopy.distance import geodesic
import math

from data_preparation_street import get_nearest_street
from finding_route_helpers import find_path_within_ellipse
from street_stats import prepare_stats_count
from utils import get_data, get_color, assign_color, get_street_path, count_delays_by_parts

G = nx.Graph()


def add_linestring_to_graph(geom, label):
    nodes = list(geom.coords)
    for i in range(len(nodes) - 1):
        u, v = nodes[i], nodes[i+1]
        dist = Point(u).distance(Point(v))
        G.add_edge(u, v, weight=dist, label=label)


def create_graph(gdf):
    for _, row in gdf.iterrows():
        geom = row['geometry']
        label = row['nazev']
        if isinstance(geom, LineString):
            add_linestring_to_graph(geom, label)
    # nx.write_graphml(G, "datasets/road_network_with_labels.graphml")


def load_graph():
    G = nx.read_graphml("datasets/road_network_with_labels.graphml")
    return G


def get_street_first_coor(street_name: str, df: GeoDataFrame):
    street_df = df[df['nazev_x'] == street_name]
    geom = street_df['geometry'].iloc[0]
    coords = None
    if isinstance(geom, LineString):
        coords = geom.coords[0]
    elif isinstance(geom, MultiLineString):
        geom = geom.geoms[0]
        coords = geom.coords[0]
    return coords


def heuristic(node, target):
    return Point(node).distance(Point(target))


def prepare_data_from_path(streets_gdf: GeoDataFrame, route: LineString,  original_streets: list,
                           from_time: str, to_time: str):
    streets_gdf['is_route'] = streets_gdf.apply(lambda row: row['geometry'].intersects(route), axis=1)
    df_routes = streets_gdf[streets_gdf['is_route']]
    df_routes['intersection'] = df_routes.apply(lambda row: intersection(row['geometry'], route), axis=1)
    df_routes['not_point_intersection'] = df_routes.apply(lambda row: not isinstance(row['intersection'], Point),
                                                          axis=1)

    df_routes_intersection = df_routes[df_routes['not_point_intersection']]
    streets = list(set(df_routes_intersection['nazev'].values.tolist()))

    gdf_final = df_routes_intersection
    gdf_final = gdf_final.drop(['is_route', 'intersection', 'not_point_intersection'], axis=1)
    streets = list(set(streets + original_streets))
    data = get_data(from_time, to_time, out_fields="pubMillis,street", out_streets=streets)

    if data.empty:
        # If data is empty, create a new DataFrame
        # data = pd.DataFrame({'street': streets, 'count': [0] * len(streets)})
        gdf_final['count'] = 0
        data = gdf_final
    df_count = count_delays_by_parts(gdf_final, data)
    return df_count


def find_route_by_streets(src_street: str, dst_street: str, from_time: str, to_time: str, gdf: GeoDataFrame,
                          streets_gdf: GeoDataFrame):
    source = get_street_first_coor(src_street, gdf)
    destination = get_street_first_coor(dst_street, gdf)
    try:
        path = nx.astar_path(G, source, destination, heuristic=heuristic, weight='weight')
    except Exception as e:
        return [], [], []
    path_coordinates = [(x, y) for x, y in path]
    route = LineString(path_coordinates)

    street_geometry_dict = []
    original_streets = [src_street, dst_street]
    gdf_final, streets, df_count = prepare_data_from_path(streets_gdf, route, original_streets, from_time, to_time )

    for street in streets:
        final_dict = {'street_name': street,
                      'path': get_street_path(gdf_final, street, from_time, to_time),
                      'color': get_color(df_count, street, 'nazev')}

        street_geometry_dict += [final_dict]

    return path_coordinates, list(set(streets)), street_geometry_dict


def get_distance(coord1, coord2):
    return geodesic(coord1, coord2).meters


def find_nearest_point(coord):
    """
    Finds the nearest point in graph to the point coordinate
    :param coord: given coordinate
    :return: the nearest coordinate in graph
    """
    min_source = None
    min_distance = math.inf
    for node in G.nodes:
        x = get_distance(node, coord)
        if x < min_distance:
            min_distance = x
            min_source = node
    return min_source


def midpoint(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    # lat1 = radians(lat1)
    # lon1 = radians(lon1)
    # lat2 = radians(lat2)
    # lon2 = radians(lon2)

    # Haversine formula to calculate distance between points
    # dlon = lon2 - lon1
    # dlat = lat2 - lat1
    # a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    # c = 2 * atan2(sqrt(a), sqrt(1 - a))
    # distance = 6371 * c  # Radius of the Earth in kilometers

    # Midpoint calculation
    mid_lat = (lat1 + lat2) / 2
    mid_lon = (lon1 + lon2) / 2

    return mid_lat, mid_lon


def find_route_by_coord(src_coord, dst_coord,
                        from_time: str, to_time: str, streets_gdf: GeoDataFrame,
                        grid_gdf: GeoDataFrame, merged_gdf_streets: GeoDataFrame):
    source = find_nearest_point(src_coord)
    destination = find_nearest_point(dst_coord)
    try:
        long_source, lat_source = source
        long_dst, lat_dst = destination
        long, lat = midpoint(lat_source, long_source, lat_dst, long_dst)
        ellipse_center = (lat, long)
        major_axis = abs(lat_source - lat_dst) / 2
        minor_axis = abs(long_source - long_dst) /2

        path = find_path_within_ellipse(G, source, destination, ellipse_center, major_axis, minor_axis)

    except nx.NetworkXNoPath:
        return [], [], []

    path_coordinates = [(x, y) for x, y in path]
    path_coordinates = [src_coord] + path_coordinates + [dst_coord]  # todo: toto lepsie z df
    route = LineString(path_coordinates)

    source_street = get_nearest_street(source, grid_gdf, merged_gdf_streets, streets_gdf)
    dst_street = get_nearest_street(destination, grid_gdf, merged_gdf_streets, streets_gdf)

    street_geometry_dict = []
    df_count = prepare_data_from_path(streets_gdf, route, [source_street, dst_street],
                                                        from_time, to_time)
    df_count.rename(columns={"street": "nazev"}, inplace=True)
    for index, row in df_count.iterrows():
        # path, color = get_street_path(gdf_final, street, from_time, to_time)

        final_dict = {'street_name': row['nazev'],
                      'path': [[long, lat] for lat, long in row['geometry'].coords],
                      'color': row['color']}

        street_geometry_dict += [final_dict]
    return route, street_geometry_dict, source_street, dst_street
