import earthkit.data
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

import argparse

def main():

    parser = argparse.ArgumentParser(description='Retrieve ERA5 data')

    parser.add_argument('--store_path', type=str, help='Absoluthe path of ERA store', required=True)
    parser.add_argument('--type', type=str, help='single-levels/land', required=True)
    parser.add_argument('--start', type=str, help='Start year for the data retrieval', required=True)
    parser.add_argument('--end', type=str, help='End year for the data retrieval', required=True)

    parser.add_argument('--bbox', help='Bounding box for the data retrieval N,W,S,E', required=True, nargs='+', type=float)

    # [45.5, 8, 43, 13]
    

    args = parser.parse_args()


    resolution = [0.1, 0.1] if args.type == 'land' else [0.25, 0.25] 

    # Make a list of hours in the day
    hours = [f"{i:02d}" for i in range(24)]

    dates = pd.date_range(start=f"{args.start}-01-01", end=f"{args.end}-12-31").strftime("%Y-%m-%d").tolist()

    try:
        last = xr.open_zarr(fr'{args.store_path}').time.values.max()
    except:
        last = pd.to_datetime("1930-01-01")

    for date in dates:
        if pd.to_datetime(date) < last:
            continue
            # Retrieve all the dates in the year
        ds = earthkit.data.from_source("cds",
                                        f"reanalysis-era5-{args.type}",
                                        variable=["2m_temperature","total_precipitation"],
                                        product_type="reanalysis",
                                        area=args.bbox, 
                                        grid=resolution,
                                        date=[date],
                                        time=hours
                                    )
    

        for ds_param in ds.group_by('param'):

            if '2t' in ds_param.metadata('param'):

                # Convert to xarray
                ds_xarray = ds_param.to_xarray().mean(['number','surface','step'])

                # Resample the data in tas, tasmin and tasmax to daily data - and rename
                ds_xarray_tas = ds_xarray.t2m.resample(time='D').mean().rename('tas')
                ds_xarray_tasmin = ds_xarray.t2m.resample(time='D').min().rename('tasmin')
                ds_xarray_tasmax = ds_xarray.t2m.resample(time='D').max().rename('tasmax')
            
            if 'tp' in ds_param.metadata('param'):

                # Convert to xarray
                ds_xarray = ds_param.to_xarray().mean(['number','surface','step'])
            
                ds_xarray_pr = ds_xarray.tp.resample(time='D').sum().rename('pr').sel(time=ds_xarray_tas.time)

            

        ds_to_store = xr.merge([ds_xarray_tas, ds_xarray_tasmin, ds_xarray_tasmax, ds_xarray_pr])
        ds_to_store = ds_to_store.chunk({'latitude':4, 'longitude':4, 'time':-1})

        try:
            ds_to_store.to_zarr(fr'{args.store_path}', mode='w-')

        except:

            try:
                ds_to_store.to_zarr(rf'{args.store_path}', mode='a-', append_dim='time')
            
            except:
                pass


if __name__ == "__main__":
    main()