from folium.plugins import FastMarkerCluster
from functools import partial
from shapely.ops import transform
from shapely.validation import make_valid, explain_validity
import folium
import logging
import pyproj
import shapely.geometry
import geopandas as gpd
import numpy as np
import sys


crs_3025_to_4326 = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:3035'),
    pyproj.Proj(init='epsg:4326'),
)

crs_4326_to_3035 = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:4326'),
    pyproj.Proj(init='epsg:3035'),
)


def sample_geoseries(plz, polygon, area, area_unit, overestimate=2):
    size = int(area / area_unit)
    if size <= 10:
        size = 10 # set a min sample size when area is too small
    min_x, min_y, max_x, max_y = polygon.bounds
    ratio = polygon.area / polygon.envelope.area
    samples = np.random.uniform((min_x, min_y), (max_x, max_y), (int(size / ratio * overestimate), 2))
    multipoint = shapely.geometry.MultiPoint(samples)
    multipoint = multipoint.intersection(polygon)
    samples = np.array(multipoint)
    while samples.shape[0] < size:
        # emergency catch in case by bad luck we didn't get enough within the polygon
        new_samples = sample_geoseries(plz, polygon, area, area_unit, overestimate=2)
        samples = np.concatenate([samples.reshape(-1,1), new_samples.reshape(-1,1)])
    return samples[np.random.choice(len(samples), size)]

def _save_folium_map(df):
    folium_map = folium.Map(location=[51.23238046, 13.59010037],
                            zoom_start=6,
    )

    for row in df.itertuples():
        plz = row.plz
        points = row.sampled_points
        polygon = row.geometry
        try:
            data = list(zip(points[:, 1], points[:, 0]))
            FastMarkerCluster(data=data).add_to(folium_map)
        except:
            logger.info(f'Skip one of {plz}...')
        folium.GeoJson(polygon).add_to(folium_map)
    folium_map.save(OUTPUT_MAP)


if __name__ == '__main__':
    IS_DEBUG = False
    # POLYGON_FILE = '../data_input/plz-3stellig.shp/'
    # OUTPUT_JSON = '../data_output/sampled_plz_3stellig.json'
    POLYGON_FILE = '../data_input/plz-2stellig.shp/'
    OUTPUT_JSON = '../data_output/sampled_plz_2stellig.json'
    OUTPUT_MAP = '../data_output/sampled_map.html'
    size_unit = 40

    logger = logging.getLogger(__name__)
    fh = logging.FileHandler('out.log')
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    df = gpd.read_file(POLYGON_FILE, dtype={'plz': str})
    if IS_DEBUG:
        df = df.sample(2)

    logger.info('Start to sample the polygons ...')
    df['geometry_valid'] = df['geometry'].apply(lambda x: make_valid(x))
    df['geometry_3025'] = df['geometry_valid'].apply(lambda x: transform(crs_4326_to_3035, x))
    df['area'] = df['geometry_3025'].apply(lambda x: x.area/1e6)
    df = df.loc[(df['area'] >= 0.5)]
    df['n_sampling_points'] = df['area'].apply(lambda x: int(x / size_unit))
    df['sampled_points'] = df.apply(lambda x: sample_geoseries(plz=x['plz'], polygon=x['geometry_valid'], area=x['area'], area_unit=size_unit), axis=1)

    logger.info('Dump the sampled points ...')
    df[['plz', 'sampled_points']].to_json(OUTPUT_JSON)

    if True:
        _save_folium_map(df)

    logger.info('FINISHED!')
