import math

import networkx as nx
from math import radians, sin, cos, sqrt, atan2


def manhattan_distance(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    return abs(lat1 - lat2) + abs(lon1 - lon2)


def euclidean_distance(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)


def haversine_distance(coord1, coord2):
    # Radius of the Earth in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert latitude and longitude from degrees to radians
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # Calculate the change in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Calculate the distance
    distance = R * c

    return distance


def in_ellipse(coord, center, semimajor, semiminor):
    lat, lon = coord
    lat_c, lon_c = center
    try:

        return ((lat - lat_c) ** 2) / (semimajor ** 2) + ((lon - lon_c) ** 2) / (semiminor ** 2) <= 1
    except ZeroDivisionError:
        return True


def create_ellipse_subgraph(graph, center, semimajor, semiminor):
    subgraph = nx.Graph()
    for node in graph.nodes():
        if in_ellipse(node, center, semimajor, semiminor):
            subgraph.add_node(node)
    for edge in graph.edges():
        if edge[0] in subgraph.nodes() and edge[1] in subgraph.nodes():
            subgraph.add_edge(edge[0], edge[1], weight=graph[edge[0]][edge[1]].get('weight', 1))
    return subgraph


def find_path_within_ellipse(graph, start, goal, ellipse_center, semimajor_axis, semiminor_axis):
    current_center = ellipse_center
    current_semimajor = semimajor_axis
    current_semiminor = semiminor_axis
    graph_max_len = len(graph.edges)
    while True:
        ellipse_subgraph = create_ellipse_subgraph(graph, current_center, current_semimajor, current_semiminor)
        try:
            path = nx.astar_path(ellipse_subgraph, start, goal, heuristic=euclidean_distance)
            # path = nx.astar_path(ellipse_subgraph, start, goal, heuristic=lambda u, v: haversine_distance(u, v))
            return path
        except nx.NetworkXNoPath:
            # If no path is found, increase the size of the ellipse
            current_semimajor += 0.01
            current_semiminor += 0.01
            if len(ellipse_subgraph.edges) == graph_max_len:
                raise nx.NetworkXNoPath
        except nx.NodeNotFound:
            current_semimajor += 0.01
            current_semiminor += 0.01


