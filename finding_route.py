from typing import Tuple

import networkx as nx
import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely import LineString, MultiLineString, Point, intersection
from geopy.distance import geodesic
import math

from data_preparation_street import get_nearest_street
from street_stats import prepare_stats_count
from utils import get_data, get_color, assign_color, get_street_path

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
        label = row['nazev_x']
        if isinstance(geom, LineString):
            add_linestring_to_graph(geom, label)
        elif isinstance(geom, MultiLineString):
            for segment in geom.geoms:
                add_linestring_to_graph(segment, label)
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
                           source, destination,
                           from_time: str, to_time: str):
    streets_gdf['is_route'] = streets_gdf.apply(lambda row: row['geometry'].intersects(route), axis=1)
    df_routes = streets_gdf[streets_gdf['is_route']]
    df_routes['intersection'] = df_routes.apply(lambda row: intersection(row['geometry'], route), axis=1)
    df_routes['not_point_intersection'] = df_routes.apply(lambda row: not isinstance(row['intersection'], Point),
                                                          axis=1)

    df_routes_intersection = df_routes[df_routes['not_point_intersection']]
    streets = list(set(df_routes_intersection['nazev'].values.tolist()))

    # df_routes_intersection2 = df_routes_intersection[~df_routes_intersection['nazev'].isin(original_streets)]
    # for orig_street in original_streets:
    #     df_orig_street_in_path = df_routes_intersection[df_routes_intersection['nazev'].isin([orig_street])]
    #     df_source_streets = streets_gdf[streets_gdf['nazev'].isin([orig_street])]
    # df_source_streets = streets_gdf[streets_gdf['nazev'].isin(original_streets)]
    # gdf_final = pd.concat([df_routes_intersection, df_source_streets])
    gdf_final = df_routes_intersection
    gdf_final = gdf_final.drop(['is_route', 'intersection', 'not_point_intersection'], axis=1)
    streets = list(set(streets + original_streets))
    df_count = prepare_stats_count(get_data(from_time, to_time), gdf_final)
    df_count = assign_color(df_count)
    return gdf_final, streets, df_count


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
                      'path': get_street_path(gdf_final, street),
                      'color': get_color(df_count, street, 'nazev')}

        street_geometry_dict += [final_dict]

    return path_coordinates, list(set(streets)), street_geometry_dict


def get_distance(coord1, coord2):
    return geodesic(coord1, coord2).meters


def find_nearest_point(coord):
    """
    Finds the nearest point in graph to the point coordinate
    :param coord: given coordinate (not in graph)
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


def find_route_by_coord(source: Tuple[float, float], destination: Tuple[float, float],
                        from_time: str, to_time: str, streets_gdf: GeoDataFrame,
                        grid_gdf: GeoDataFrame, merged_gdf_streets: GeoDataFrame):
    try:
        path = nx.astar_path(G, source, destination, heuristic=heuristic, weight='weight')
    except Exception as e:
        return [], [], []

    path_coordinates = [(x, y) for x, y in path]
    route = LineString(path_coordinates)

    source_street = get_nearest_street(source, grid_gdf, merged_gdf_streets, streets_gdf)
    dst_street = get_nearest_street(destination, grid_gdf, merged_gdf_streets, streets_gdf)

    street_geometry_dict = []
    gdf_final, streets, df_count = prepare_data_from_path(streets_gdf, route, [source_street, dst_street],
                                                          source, destination, from_time, to_time)

    for street in streets:
        final_dict = {'street_name': street,
                      'path': get_street_path(gdf_final, street),
                      'color': get_color(df_count, street, 'nazev')}

        street_geometry_dict += [final_dict]
    return path_coordinates, list(set(streets)), street_geometry_dict
