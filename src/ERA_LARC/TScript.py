import fsspec
import numpy as np
import os
import xarray as xr
from tqdm import tqdm
import pandas as pd
import argparse

def main():

    parser = argparse.ArgumentParser(description='Retrieve ERA5 data')

    parser.add_argument('--store_path', type=str, help='Absoluthe path of ERA store', required=True)
    parser.add_argument('--start', type=str, help='Start year for the data retrieval', required=True)
    parser.add_argument('--end', type=str, help='End year for the data retrieval', required=True)
    parser.add_argument('--bbox', help='Bounding box for the data retrieval N,W,S,E', required=True, nargs='+', type=float)

    args = parser.parse_args()


    reanalysis = xr.open_zarr(
        'gs://gcp-public-data-arco-era5/ar/1959-2022-full_37-6h-0p25deg_derived.zarr', 
        #chunks={'time':24*30},
        consolidated=True,
    )


    tas_path = os.path.join(args.store_path, 'tas')
    pr_path = os.path.join(args.store_path, 'pr')
    wind_speed_path = os.path.join(args.store_path, '10m_wind_speed')

    if not os.path.exists(tas_path):
        os.makedirs(tas_path)

    if not os.path.exists(pr_path):
        os.makedirs(pr_path)
    
    if not os.path.exists(wind_speed_path):
        os.makedirs(wind_speed_path)


    # Creazione della lista di date
    date_list = pd.date_range(start=f"{args.start}-01-01", end=f"{args.end}-12-31", freq="ME")

    lat_max, lon_min, lat_min, lon_max = args.bbox

    # Conversione delle date in stringhe con il formato desiderato
    date_str_list = date_list.strftime("%m-%Y").tolist()

    for year in date_str_list:

        if not os.path.exists(os.path.join(tas_path, f'{year}.nc')):
            try:
                reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(time=year)[['2m_temperature']].to_netcdf(os.path.join(tas_path, f'{year}.nc'))
            except:
                reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(valid_time=year)[['2m_temperature']].rename({'valid_time': 'time'}).to_netcdf(os.path.join(tas_path, f'{year}.nc'))
            
        if not os.path.exists(os.path.join(pr_path, f'{year}.nc')):
            try:
                reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(time=year)[['total_precipitation_6hr']].to_netcdf(os.path.join(pr_path, f'{year}.nc'))
            except:
                reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(valid_time=year)[['total_precipitation_6hr']].rename({'valid_time': 'time'}).to_netcdf(os.path.join(pr_path, f'{year}.nc'))
            
        if not os.path.exists(os.path.join(wind_speed_path, f'{year}.nc')):
            try:
                v = reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(time=year)[['10m_u_component_of_wind', '10m_v_component_of_wind']]
                wind_speed = np.sqrt(v['10m_u_component_of_wind']**2 + v['10m_v_component_of_wind']**2).rename('10m_wind_speed').to_dataset().to_netcdf(os.path.join(wind_speed_path, f'{year}.nc'))
            except:
                v = reanalysis.sel(latitude=slice(lat_max,lat_min), longitude=slice(lon_min,lon_max)).sel(valid_time=year)[['10m_u_component_of_wind', '10m_v_component_of_wind']]
                wind_speed = np.sqrt(v['10m_u_component_of_wind']**2 + v['10m_v_component_of_wind']**2).rename('10m_wind_speed').to_dataset().rename({'valid_time': 'time'}).to_netcdf(os.path.join(wind_speed_path, f'{year}.nc'))
        
    daily_path = os.path.join(args.store_path, 'daily')
    if not os.path.exists(daily_path):
        os.makedirs(daily_path)

    if not os.path.exists(os.path.join(daily_path, f'sfcWind.nc')):
        os.system(f'cdo chname,10m_wind_speed,sfcWind -daymean -mergetime {os.path.join(wind_speed_path, "*.nc")} {os.path.join(daily_path, "sfcWind.nc")}')
    
    if not os.path.exists(os.path.join(daily_path, f'sfcWindmax.nc')):
        os.system(f'cdo chname,10m_wind_speed,sfcWindmax -daymax -mergetime {os.path.join(wind_speed_path, "*.nc")} {os.path.join(daily_path, "sfcWindmax.nc")}')
    
    if not os.path.exists(os.path.join(daily_path, f'tas.nc')):
        os.system(f'cdo chname,2m_temperature,tas -daymean -mergetime {os.path.join(tas_path, "*.nc")} {os.path.join(daily_path, "tas.nc")}')
    
    if not os.path.exists(os.path.join(daily_path, f'tasmax.nc')):
        os.system(f'cdo chname,2m_temperature,tasmax -daymax -mergetime {os.path.join(tas_path, "*.nc")} {os.path.join(daily_path, "tasmax.nc")}')
    
    if not os.path.exists(os.path.join(daily_path, f'tasmin.nc')):
        os.system(f'cdo chname,2m_temperature,tasmin -daymin -mergetime {os.path.join(tas_path, "*.nc")} {os.path.join(daily_path, "tasmin.nc")}')

    if not os.path.exists(os.path.join(daily_path, f'pr.nc')):
        os.system(f'cdo chname,total_precipitation_6hr,pr -daysum -mergetime {os.path.join(pr_path, "*.nc")} {os.path.join(daily_path, "pr.nc")}')
    
    store_path = os.path.join(args.store_path, 'store')

    if not os.path.exists(store_path):
        os.makedirs(store_path)

    if not os.path.exists(os.path.join(store_path, f'final.nc')):
        os.system(f'cdo merge {os.path.join(daily_path, "*.nc")} {os.path.join(store_path, f"final.nc")}')
    
    with xr.open_dataset(os.path.join(store_path, 'final.nc')) as ds:
        ds = ds.chunk({'latitude':1, 'longitude':1, 'time': -1})
        ds.to_zarr(os.path.join(args.store_path, 'store.zarr'), consolidated=True)
        os.system(f'rm -r {store_path}')



if __name__ == '__main__':
    main()
    print('Finished')