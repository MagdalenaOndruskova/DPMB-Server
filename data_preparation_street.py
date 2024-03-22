from shapely import Point


def get_nearest_street(coord, grid_gdf, merged_gdf_streets, streets_gdf):
    sq_id = find_square(coord, grid_gdf)
    streets_in_square = merged_gdf_streets[merged_gdf_streets['grid_squares'].apply(lambda x: str(sq_id) in x)]
    street = find_nearest_street(coord, streets_in_square, streets_gdf)
    return street


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

    # path = get_street_path(streets_gdf, nearest_street)
    return nearest_street


def find_square(coord, grid_squares):
    point = Point(coord)

    for idx, square in grid_squares.iterrows():
        square = square.geometry
        if square.contains(point):
            return idx

    return None
