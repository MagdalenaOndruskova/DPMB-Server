import networkx as nx
from geopandas import GeoDataFrame
from shapely import LineString, MultiLineString, Point, intersection

G = nx.Graph()


def add_linestring_to_graph(geom):
    nodes = list(geom.coords)
    for i in range(len(nodes) - 1):
        u, v = nodes[i], nodes[i+1]
        dist = Point(u).distance(Point(v))
        G.add_edge(u, v, weight=dist)


def create_graph(gdf):
    for _, row in gdf.iterrows():
        geom = row['geometry']
        if isinstance(geom, LineString):
            add_linestring_to_graph(geom)
        elif isinstance(geom, MultiLineString):
            for segment in geom.geoms:
                add_linestring_to_graph(segment)


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

    for street in streets:
        if street in original_streets:
            continue
        df_streets = df_routes_intersection[df_routes_intersection['nazev'] == street]
        path = None
        for index, row in df_streets.iterrows():
            geometry = row['geometry']
            coordinates = geometry.coords
            coordinates2 = [[long, lat] for lat, long in coordinates]
            if not path:
                path = [coordinates2]
            else:
                path = path + [coordinates2]
        final_dict = {'street_name': street,
                      'path': path}
        street_geometry_dict += [final_dict]

    streets_gdf_selected = streets_gdf[streets_gdf['nazev'].isin(original_streets)]
    for street in original_streets:
        path = None
        for index, row in streets_gdf_selected.iterrows():
            geometry = row['geometry']
            coordinates = geometry.coords
            coordinates2 = [[long, lat] for lat, long in coordinates]
            if not path:
                path = [coordinates2]
            else:
                path = path + [coordinates2]
        final_dict = {'street_name': street,
                      'path': path}
        street_geometry_dict += [final_dict]

    return path_coordinates, list(set(streets)), street_geometry_dict

