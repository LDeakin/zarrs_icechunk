#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "xarray==2025.1.2",
#     "numpy==2.2.3",
#     "netcdf4==1.7.2",
#     "h5netcdf==1.6.1",
#     # "dask==2025.2.0",
# ]
# ///

import numpy as np
import xarray as xr

shape = (2, 5, 10)
data = np.arange(np.prod(shape), dtype=np.uint32).reshape(shape)
da = xr.DataArray(data, dims=("z", "y", "x")) # .chunk({"z": 1, "y": 3, "x": 2})

encoding = {"data": {
    "shuffle": True,
    # "scale_factor": 0.1 # TODO
    "zlib": True,
    "chunksizes": (1, 2, 2),
    "fletcher32": True,
    # TODO: Quantize, delta?
}}

ds = xr.Dataset({"data": da})
ds.to_netcdf('data/test0.nc', encoding=encoding)

ds = xr.Dataset({"data": da + 100})
ds.to_netcdf('data/test1.nc', encoding=encoding)
