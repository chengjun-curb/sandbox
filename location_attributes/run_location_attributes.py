#!/usr/bin/env python
# coding: utf-8

from pandarallel import pandarallel
pandarallel.initialize(nb_workers=32, progress_bar=True)

import multiprocessing as mp

import numpy as np
import geopandas as gpd
import shapely.geometry

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from functools import partial

import pyproj
from shapely import geometry
from shapely.geometry import Point, Polygon
from shapely.ops import transform
import pickle
import pandas as pd
from matplotlib.pyplot import cm
import numpy as np
import shapely.ops as ops

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
# from folium.plugins import FastMarkerCluster


from shapely.validation import explain_validity
from shapely.validation import make_valid


def get_area_polygon(center, radius):
    lat = center[0]
    lon = center[1]
    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, lon
    )
    
    
    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(local_azimuthal_projection),
    )
    
    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection),
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
#         pyproj.Proj(init='epsg:3035'), # destination # DE
    
    )
    
    center_point = Point(float(lon), float(lat))
    point_transformed = transform(wgs84_to_aeqd, center_point)
    
    #TODO: HERE FOR DIFFERENT SHAPE
    buffer = point_transformed.buffer(radius, resolution=10)
    
    
    # Get the polygon with lat lon coordinates
    circle_poly = transform(aeqd_to_wgs84, buffer)
    
    return circle_poly


def get_polygon(center, radius):
    lat = center[0]
    lon = center[1]
    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, lon
    )
    
    
    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(local_azimuthal_projection),
    )
    
    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection),
#         pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(init='epsg:3035'), # destination # DE
    
    )
    
    center_point = Point(float(lon), float(lat))
    point_transformed = transform(wgs84_to_aeqd, center_point)
    
    #TODO: HERE FOR DIFFERENT SHAPE
    buffer = point_transformed.buffer(radius, resolution=10)
    
    
    # Get the polygon with lat lon coordinates
    circle_poly = transform(aeqd_to_wgs84, buffer)
    
    return circle_poly


def get_area(polygon):
    geom_area = ops.transform(
        partial(
            pyproj.transform,
            pyproj.Proj(init='EPSG:4326'),

            pyproj.Proj(
                proj='aea',
                lat_1=polygon.bounds[1],
                lat_2=polygon.bounds[3]
            )
        ),
        polygon)
    
    # Print the area in KM^2
    area = geom_area.area / 10**6
    return area


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



def get_pip(points: pd.DataFrame, polygon_3035: Polygon) -> pd.DataFrame:
    lon_col = 'x_mp_100m'
    lat_col = 'y_mp_100m'
    geometry = gpd.points_from_xy(points[lon_col], points[lat_col], crs = "EPSG:3035")
    gdf_points = gpd.GeoDataFrame(points, geometry=geometry, crs = "EPSG:3035")
        
    mask = gdf_points.within(polygon_3035)
    df_pip = gdf_points.loc[mask]
    return df_pip



def get_attribute(df: pd.DataFrame) -> pd.DataFrame:
    RADIUS = 3000 # meter
    for i, l in enumerate(locations):
        center = locations[l]
        circle_poly = get_polygon(center, RADIUS)
        df_pip = get_pip(points=df, polygon=circle_poly)
        
        data = (df_pip.groupby('Auspraegung_Text')['Gitter_ID_100m']
            .count()
            .reset_index() 
            .rename(columns={'Gitter_ID_100m': f'{l}'})
            .set_index('Auspraegung_Text')
            .T
           )
        
        if i == 0:
            df_data = data
        else:
            df_data = pd.concat([df_data, data], axis=0, ignore_index=False)
    return df_data
    


# # read in the population

# In[3]:


# population
#df_pop = pd.read_csv('./Zensus_Bevoelkerung_100m-Gitter.csv', sep=";")


# Each plz could have multiple Polygon!
plz_shape_df = gpd.read_file('./plz-gebiete.shp/', dtype={'plz': str})
plz_shape_df.drop(['note'], axis=1, inplace=True)


# Map Ort to plz, it is many to one mapping.
# Use the list of Ort as city name
df_landkreis = pd.read_csv(
    './zuordnung_plz_ort_landkreis.csv', 
    sep=',', 
    dtype={'plz': str}
)
df_landkreis.drop('osm_id', axis=1, inplace=True)

df_plz_ort = df_landkreis.groupby(['plz'])['ort'].apply(set).reset_index()
df_plz_ort['city_name'] = df_plz_ort.ort.apply(str)
# df_plz_ort = df_plz_ort.rename(columns={'ort': 'city_name'})
df_plz_ort['city_name'] = df_plz_ort.city_name.str.replace('{', '')
df_plz_ort['city_name'] = df_plz_ort.city_name.str.replace('}', '')




germany_df = pd.merge(
    left=df_plz_ort, 
    right=plz_shape_df, 
    on='plz',
    how='left'
)


###################
# Debug
###################
# germany_df = germany_df.loc[germany_df.city_name.isin(["'Berlin'", "'Essen'"])].sample(20)
# germany_df = germany_df.loc[germany_df.city_name.isin(["'Berlin'", "'Essen'"])].sample(20)
germany_df = germany_df.loc[(germany_df.city_name == "'Berlin'") | (germany_df.city_name == "'Essen'")].sample(15)



def get_pip_pop(points: pd.DataFrame, polygon_3035: Polygon) -> pd.DataFrame:
    lon_col = 'x_mp_100m'
    lat_col = 'y_mp_100m'
    geometry = gpd.points_from_xy(points[lon_col], points[lat_col], crs = "EPSG:3035")
    gdf_points = gpd.GeoDataFrame(points, geometry=geometry, crs = "EPSG:3035")

    mask = gdf_points.within(polygon_3035)
    df_pip = gdf_points.loc[mask]
    return df_pip


def get_pip(points: pd.DataFrame, lon_col, lat_col, polygon: Polygon) -> pd.DataFrame:
    geometry = gpd.points_from_xy(points[lon_col], points[lat_col], crs = "EPSG:3035")
    gdf_points = gpd.GeoDataFrame(points, geometry=geometry, crs = "EPSG:3035")

    mask = gdf_points.within(polygon)
    df_pip = gdf_points.loc[mask]
    return df_pip



def sample_geoseries(plz, polygon, area, area_unit, overestimate=2):
#     print(plz)
#     size = int(area / area_unit)
    size = 10
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
        samples = np.concatenate([samples, sample_geoseries(plz, polygon, area, area_unit, overestimate=2)])
    return samples[np.random.choice(len(samples), size)]





size_unit = 4


germany_df['geometry_valid'] = germany_df['geometry'].apply(lambda x: make_valid(x))
germany_df['geometry_3025'] = germany_df['geometry_valid'].apply(lambda x: transform(crs_4326_to_3035, x))
germany_df['area'] = germany_df['geometry_3025'].apply(lambda x: x.area/1e6)

###############################
# deprecated due to slowness
###############################
# parallel version
def get_n_pop(df):
    df_pip = get_pip_pop(points=df_pop, polygon_3035=df.geometry_3025)   
    df_pip = df_pip.loc[(df_pip.Einwohner != -1)]
    n = df_pip.Einwohner.sum()
    return n

#print('population: start parallel...')
#germany_df['population'] = germany_df.parallel_apply(get_n_pop, axis=1)


# germany_df['n_sampling_points'] = germany_df['area'].apply(lambda x: int(x / size_unit))
# germany_df['sampled_points'] = germany_df.apply(lambda x: sample_geoseries(plz=x['plz'], polygon=x['geometry_valid'], area=x['area'], area_unit=size_unit), axis=1)




# "EPSG:3035"
# 'EPSG:4326'
def get_pip(points: pd.DataFrame, lon_col, lat_col, polygon: Polygon, crs=None) -> pd.DataFrame:
    geometry = gpd.points_from_xy(points[lon_col], points[lat_col], crs=crs)
    gdf_points = gpd.GeoDataFrame(points, geometry=geometry, crs=crs)
    mask = gdf_points.within(polygon)
    df_pip = gdf_points.loc[mask]
    return df_pip


# - Add offline restaurants (gastronomie) 

df_offline = pd.read_csv('./gastronomie.csv')
df_offline = df_offline.drop_duplicates()
df_offline = df_offline.dropna(subset=["Latitude", "Longitude"])

#def get_n_offline(x):
#    df_pip = get_pip(points=df_offline, lon_col='Longitude', lat_col='Latitude', polygon=x)
#    return len(df_pip)

#germany_df['n_offline_restaurant'] = germany_df['geometry_valid'].apply(lambda x: get_n_offline(x))

# parallel
def get_n_offline(df):    
    df_pip = get_pip(points=df_offline, lon_col='Longitude', lat_col='Latitude', polygon=df.geometry_valid)
    return len(df_pip)

print('start parallel...')
germany_df['n_offline_restaurant'] = germany_df.parallel_apply(get_n_offline, axis=1)

print('hi2')

# - Add online restaurants (lieferando)

# In[74]:


df_online = pd.read_csv('./lieferando_restaurant.csv')
df_online = df_online.drop_duplicates()
df_online = df_online.dropna(subset=["lat", "lng"])

# parallel
def get_n_online(df):    
    df_pip = get_pip(points=df_online, lon_col='lng', lat_col='lat', polygon=df.geometry_valid)
    return len(df_pip)

print('start parallel...')
germany_df['n_online_restaurant'] = germany_df.parallel_apply(get_n_online, axis=1)


#def get_n_online(x):
#    df_pip = get_pip(points=df_online, lon_col='lng', lat_col='lat', polygon=x)
#    return len(df_pip)

#germany_df['n_online_restaurant'] = germany_df['geometry_valid'].apply(lambda x: get_n_online(x))

print('hi3')

# - Add wolt
df_wolt = pd.read_csv('./wolt_restaurant_2.csv')
df_wolt = df_wolt.drop_duplicates()
df_wolt = df_wolt.dropna(subset=["lat", "lon"])

# parallel
def get_n_wolt(df):    
    df_pip = get_pip(points=df_wolt, lon_col='lon', lat_col='lat', polygon=df.geometry_valid)
    return len(df_pip)

print('start parallel...')
germany_df['n_wolt'] = germany_df.parallel_apply(get_n_wolt, axis=1)


#def get_n_wolt(x):
#    df_pip = get_pip(points=df_wolt, lon_col='lon', lat_col='lat', polygon=x)
#    return len(df_pip)

#germany_df['n_wolt'] = germany_df['geometry_valid'].apply(lambda x: get_n_wolt(x))


# - Add foodpanda

# In[76]:


df_foodpanda = pd.read_csv('./foodpanda_code.csv')
df_foodpanda = df_foodpanda.drop_duplicates()
df_foodpanda = df_foodpanda.dropna(subset=["latitude", "longitude"])

# parallel
def get_n_foodpanda(df):    
    df_pip = get_pip(points=df_foodpanda, lon_col='longitude', lat_col='latitude', polygon=df.geometry_valid)
    return len(df_pip)

print('start parallel...')
germany_df['n_foodpanda'] = germany_df.parallel_apply(get_n_foodpanda, axis=1)

#def get_n_foodpanda(x):
#    df_pip = get_pip(points=df_foodpanda, lon_col='longitude', lat_col='latitude', polygon=x)
#    return len(df_pip)

#germany_df['n_foodpanda'] = germany_df['geometry_valid'].apply(lambda x: get_n_foodpanda(x))




# - Add Uber eats
df_ubereats = pd.read_csv('./ubereats_stores.csv')
df_ubereats = df_ubereats.drop_duplicates()
df_ubereats = df_ubereats.dropna(subset=["latitude", "longitude"])



# parallel
def get_n_ubereats(df):
    df_pip = get_pip(points=df_wolt, lon_col='lon', lat_col='lat', polygon=df.geometry_valid)
    return len(df_pip)

print('start parallel...')
germany_df['n_ubereats'] = germany_df.parallel_apply(get_n_ubereats, axis=1)



# def get_n_ubereats(x):
    # df_pip = get_pip(points=df_ubereats, lon_col='longitude', lat_col='latitude', polygon=x)
    # return len(df_pip)

# germany_df['n_ubereats'] = germany_df['geometry_valid'].apply(lambda x: get_n_ubereats(x))









# # Plz level table

# Note: group by the plz first and then do the join

# In[84]:


# Add income

df_income = pd.read_csv('./income.csv', index_col=0).reset_index()
df_income = df_income.rename(columns={'index': 'plz'})
df_income.plz = df_income.plz.astype(str)

df_master = pd.merge(
    left=germany_df, 
    right=df_income, 
    on='plz',
    how='left'
)


# In[ ]:





# In[85]:


# add population


plz_einwohner_df = pd.read_csv(
     './plz_einwohner.csv', 
     sep=',', 
     dtype={'plz': str, 'einwohner': int}
)
plz_einwohner_df = plz_einwohner_df.rename(columns={'einwohner': 'population'})




df_master = pd.merge(
     left=df_master, 
     right=plz_einwohner_df, 
     on='plz',
     how='left'
)

df_master = df_master.loc[(df_master['area'] >= 0.2 )]

# In[87]:


# df_master.sort_values(by='population_density', ascending=False)


# In[88]:


df_master_plz = df_master.copy()


# In[89]:


df_master_plz.head()


# In[90]:


df_master_plz['area'] = round(df_master_plz.area, 3)
df_master_plz = df_master_plz.loc[df_master_plz['area'] != 0]


# In[91]:


df_master_plz = df_master_plz.rename(columns={'apartment_price_index (price_polygon / avg_price_8_big_city)': 'purchase_power_index'})

df_master_plz['purchase_power_index'] = round(df_master_plz['purchase_power_index'].fillna(0), 2)

# In[92]:


df_master_plz['population_per_km2'] = df_master_plz['population'] / df_master_plz['area']
#import ipdb; ipdb.set_trace()
#df_master_plz['population_per_km2'] = df_master_plz['population_per_km2'].replace([np.inf, -np.inf], np.nan, inplace=True)
#df_master_plz['population_per_km2'] = df_master_plz['population_per_km2'].fillna(0)
df_master_plz['population_per_km2'] = df_master_plz['population_per_km2'].astype(int)

df_master_plz['online_restaurant_per_km2'] = round(df_master_plz['n_online_restaurant'] / df_master_plz['area'], 1)
df_master_plz['offline_restaurant_per_km2'] = round(df_master_plz['n_offline_restaurant'] / df_master_plz['area'], 1)

df_master_plz['online_restaurant_per_1000_capita'] = round((df_master_plz['n_online_restaurant'] / df_master_plz['population'])*1e3, 2)
df_master_plz['offline_restaurant_per_1000_capita'] = round((df_master_plz['n_offline_restaurant'] / df_master_plz['population'])*1e3, 2)



df_master_plz['plz_3'] = df_master_plz['plz'].str[:3]

df_grouped = df_master_plz.groupby(['plz_3', 'city_name']).agg({'area': sum,
    'population': sum,
    'population_per_km2': np.mean,
    'purchase_power_index': np.mean,
    'n_wolt': sum,
    'n_foodpanda': sum,
    'n_ubereats': sum,
    'n_offline_restaurant': sum,
    'n_online_restaurant': sum,
    'offline_restaurant_per_km2': np.mean,
    'online_restaurant_per_km2': np.mean,
    'offline_restaurant_per_1000_capita': np.mean,
    'online_restaurant_per_1000_capita': np.mean,
    })
df_grouped = df_grouped.reset_index()




df_grouped.loc[df_grouped['n_wolt'] > 0, 'is_wolt'] = 'Yes'
df_grouped.loc[df_grouped['n_wolt'] == 0, 'is_wolt'] = 'No'

df_grouped.loc[df_grouped['n_foodpanda'] > 0, 'is_foodpanda'] = 'Yes'
df_grouped.loc[df_grouped['n_foodpanda'] == 0, 'is_foodpanda'] = 'No'


df_grouped.loc[df_grouped['n_ubereats'] > 0, 'is_ubereats'] = 'Yes'
df_grouped.loc[df_grouped['n_ubereats'] == 0, 'is_ubereats'] = 'No'




df_grouped = df_grouped.rename(columns={'area': 'area (km2)'})

cols = ['plz_3',
    'city_name',
    'area (km2)',
    'population', 
    'population_per_km2',
    'purchase_power_index',
    'is_wolt',
    'is_foodpanda',
    'is_ubereats',
    #         'is_gorillas',
    #         'is_goflink',
    'n_offline_restaurant',          
    'n_online_restaurant',
    'offline_restaurant_per_km2',
    'online_restaurant_per_km2',
    'offline_restaurant_per_1000_capita',
    'online_restaurant_per_1000_capita',
]
df_grouped = df_grouped[cols]




grouped = df_grouped.groupby('city_name')
for name, group in grouped:
    fname = name.replace('/', '')[:10]
    group.to_csv(f'./data_cities/{fname}.csv', index=False)



# # City level table
df_city = df_master.groupby('city_name')['population', 'area', 'n_offline_restaurant', 'n_online_restaurant', 'n_wolt', 'n_foodpanda', 'n_ubereats'].sum().sort_values(by='population', ascending=False).reset_index()


# In[98]:


# - Add income

# In[99]:


df_income_city = df_master.groupby('city_name')['apartment_price_index (price_polygon / avg_price_8_big_city)'].mean().reset_index()

df_city = df_city.merge(df_income_city, on='city_name', how='left')
df_city = df_city.rename(columns={'apartment_price_index (price_polygon / avg_price_8_big_city)': 'purchase_power_index'})
df_city['purchase_power_index'] = round(df_city['purchase_power_index'].fillna(0), 2)



# - Add density per km2

# In[101]:


df_city['population_per_km2'] = df_city['population'] / df_city['area']
#df_city['population_per_km2'] = df_city['population_per_km2'].replace([np.inf, -np.inf], np.nan, inplace=True)
#df_city['population_per_km2'] = df_city['population_per_km2'].fillna(0)
df_city['population_per_km2'] = df_city['population_per_km2'].astype(int)

df_city['online_restaurant_per_km2'] = round(df_city['n_online_restaurant'] / df_city['area'], 1)
df_city['offline_restaurant_per_km2'] = round(df_city['n_offline_restaurant'] / df_city['area'], 1)


# - Add online and offline per 1000 captial

# In[102]:


df_city['online_restaurant_per_1000_capita'] = round((df_city['n_online_restaurant'] / df_city['population'])*1e3, 2)
df_city['offline_restaurant_per_1000_capita'] = round((df_city['n_offline_restaurant'] / df_city['population'])*1e3, 2)


# - is_wolt and is_foodpanda

# In[103]:


df_city.loc[df_city['n_wolt'] > 0, 'is_wolt'] = 'Yes'
df_city.loc[df_city['n_wolt'] == 0, 'is_wolt'] = 'No'

df_city.loc[df_city['n_foodpanda'] > 0, 'is_foodpanda'] = 'Yes'
df_city.loc[df_city['n_foodpanda'] == 0, 'is_foodpanda'] = 'No'

df_city.loc[df_city['n_ubereats'] > 0, 'is_ubereats'] = 'Yes'
df_city.loc[df_city['n_ubereats'] == 0, 'is_ubereats'] = 'No'

# - is_goflink and is_gorillas

# In[104]:


df = pd.read_json('./goflink.jl', lines=True)
goflink_set = set(df['germany'][0]) | set(df['germany'][1])

df = pd.read_json('./gorillas.jl', lines=True)
gorillas_set = set(df.loc[0, 'Germany']) | set(df.loc[1, 'Deutschland'])


df = pd.read_csv('./Scoober_Städte_Lieferando_2021_corrected.csv')
lieferando_set = set(df.city_name)



goflink_set = set([i.lower() for i in goflink_set])
gorillas_set = set([i.lower() for i in gorillas_set])
lieferando_set = set([i.lower() for i in lieferando_set])


# Hard fix
goflink_set |= {'frankfurt am main', 'freiburg im breisgau'}
gorillas_set |= {'freiburg im breisgau'}

def format_city(name):
    return set(name.replace("'", "").replace(' ', '').split(','))

def check_goflink(subset):
    return subset <= goflink_set

def check_gorillas(subset):
    return subset <= gorillas_set

def check_lieferando(subset):
    return subset <= lieferando_set





df_city['city_lower'] = df_city['city_name'].str.lower()
df_city['CITY'] = df_city['city_lower'].apply(format_city)

df_city['tf_goflink'] = df_city['CITY'].apply(check_goflink)
df_city['tf_gorillas'] = df_city['CITY'].apply(check_gorillas)
df_city['tf_lieferando'] = df_city['CITY'].apply(check_lieferando)


df_city.loc[df_city['tf_goflink'] == True, 'is_goflink'] = 'Yes'
df_city.loc[df_city['tf_goflink'] == False, 'is_goflink'] = 'No'

df_city.loc[df_city['tf_gorillas'] == True, 'is_gorillas'] = 'Yes'
df_city.loc[df_city['tf_gorillas'] == False, 'is_gorillas'] = 'No'

df_city.loc[df_city['tf_lieferando'] == True, 'is_lieferando'] = 'Yes'
df_city.loc[df_city['tf_lieferando'] == False, 'is_lieferando'] = 'No'


df_city = df_city.drop(columns=['city_lower', 'CITY', 'tf_goflink', 'tf_gorillas', 'tf_lieferando'])





# - Add tier
tier_def = [0, 4e4, 1e5, 3e5, 1.5e6, np.inf]
labels = [i for i in range(1, len(tier_def))]
labels.reverse()

df_city['tier'] = pd.cut(df_city['population'], tier_def, labels=labels, right=False)


# - Cleaning and export to csv
cols = ['city_name', 
        'tier', 
        'population', 
        'population_per_km2',
        'purchase_power_index',
        'is_wolt',
        'is_ubereats',
        'is_foodpanda',
        'is_lieferando',
        'is_gorillas',
        'is_goflink',
        'n_offline_restaurant',          
        'n_online_restaurant',
        'offline_restaurant_per_km2',
        'online_restaurant_per_km2',
        'offline_restaurant_per_1000_capita',
        'online_restaurant_per_1000_capita',
       ]
df_city[cols].to_csv('city_level.csv', index=False)
