#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "xarray==2025.1.2",
#     "icechunk==0.2.7",
#     "virtualizarr==1.3.2",
#     "h5netcdf==1.6.1",
# ]
# ///

import os

import xarray as xr
import icechunk
from virtualizarr import open_virtual_dataset

vds = [
    open_virtual_dataset('data/test0.nc'),
    open_virtual_dataset('data/test1.nc'),
]
combined_vds = xr.concat(vds, dim='z', coords='minimal', compat='override')

storage = icechunk.local_filesystem_storage("examples/data/test.icechunk.zarr")
repo = icechunk.Repository.create(storage=storage)
session = repo.writable_session(branch="main")
combined_vds.virtualize.to_icechunk(session.store)
session.commit("Initial commit")
