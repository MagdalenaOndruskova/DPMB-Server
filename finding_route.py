import networkx as nx
import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely import LineString, MultiLineString, Point, intersection

from street_stats import prepare_stats_count
from utils import get_data, get_color, assign_color

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
    nx.write_graphml(G, "datasets/road_network_with_labels.graphml")


def load_graph():
    global G
    G = nx.read_graphml("datasets/road_network_with_labels.graphml")


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


def find_route_by_streets(src_street: str, dst_street: str, gdf: GeoDataFrame, streets_gdf: GeoDataFrame):
    source = get_street_first_coor(src_street, gdf)
    destination = get_street_first_coor(dst_street, gdf)
    try:
        path = nx.astar_path(G, source, destination, heuristic=heuristic, weight='weight')
    except Exception as e:
        return [], [], []
    path_coordinates = [(x, y) for x, y in path]
    route = LineString(path_coordinates)

    streets_gdf['is_route'] = streets_gdf.apply(lambda row: row['geometry'].intersects(route), axis=1)
    df_routes = streets_gdf[streets_gdf['is_route']]
    df_routes['intersection'] = df_routes.apply(lambda row: intersection(row['geometry'], route), axis=1)
    df_routes['not_point_intersection'] = df_routes.apply(lambda row: not isinstance(row['intersection'], Point), axis=1)

    df_routes_intersection = df_routes[df_routes['not_point_intersection']]
    streets = list(set(df_routes_intersection['nazev'].values.tolist()))

    street_geometry_dict = []
    original_streets = [src_street, dst_street]
    df_routes_intersection = df_routes_intersection[~df_routes_intersection['nazev'].isin(original_streets)]
    df_source_streets = streets_gdf[streets_gdf['nazev'].isin(original_streets)]
    gdf_final = pd.concat([df_routes_intersection, df_source_streets])
    gdf_final = gdf_final.drop(['is_route', 'intersection', 'not_point_intersection'], axis=1)
    streets = list(set(streets + original_streets))
    df_count = prepare_stats_count(get_data(), gdf_final)
    df_count = assign_color(df_count)
    for street in streets:
        df_streets = gdf_final[gdf_final['nazev'] == street]
        path = None
        for index, row in df_streets.iterrows():
            geometry = row['geometry']
            coordinates = geometry.coords
            coordinates2 = [[long, lat] for lat, long in coordinates]
            if not path:
                path = [coordinates2]
            else:
                path = path + [coordinates2]
        # TODO: color

        color = get_color(df_count, street, 'nazev')
        final_dict = {'street_name': street,
                      'path': path,
                      'color': color}

        street_geometry_dict += [final_dict]

    return path_coordinates, list(set(streets)), street_geometry_dict

