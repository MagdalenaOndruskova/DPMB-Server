from shapely import Point


def find_nearest_street(coord, street_data, streets_gdf):
    point = Point(coord)
    nearest_distance = float('inf')
    nearest_street = None

    for index, row in street_data.iterrows():
        if row['geometry'] is None:
            continue
        distance = row['geometry'].distance(point)
        if distance < nearest_distance:
            nearest_distance = distance
            nearest_street = row['nazev_x']

    street_found_gdf = streets_gdf[streets_gdf['nazev'] == nearest_street]
    path = None
    for index, row in street_found_gdf.iterrows():
        geometry = row['geometry']
        coordinates = geometry.coords
        coordinates2 = [[long, lat] for lat, long in coordinates]
        if not path:
            path = [coordinates2]
        else:
            path = path + [coordinates2]

    return nearest_street, path


def find_square(coord, grid_squares):
    point = Point(coord)

    for idx, square in grid_squares.iterrows():
        square = square.geometry
        if square.contains(point):
            return idx

    return None
