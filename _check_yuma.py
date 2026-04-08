import sqlite3, json, sys, os
sys.path.insert(0, os.getcwd())
conn = sqlite3.connect('mvp_backend/airspace_data/airspace.sqlite')

# Simulate what _rasterize_airspace does for a grid crossing R-2307
from mvp_backend.planner import GridSpec, _rasterize_airspace, _point_in_polygon
import math

# Create a small grid covering the R-2307 area
lat0, lon0 = 32.7, -114.5
lat1, lon1 = 33.1, -113.5
dlat = 0.009  # ~1km
dlon = 0.011
n_lat = int(math.ceil((lat1 - lat0) / dlat)) + 1
n_lon = int(math.ceil((lon1 - lon0) / dlon)) + 1
print(f"Grid: {n_lat}x{n_lon} = {n_lat*n_lon} cells")

grid = GridSpec(lat0=lat0, lon0=lon0, n_lat=n_lat, n_lon=n_lon, dlat=dlat, dlon=dlon)

# Build flat passable and elev arrays
passable = [[True] * n_lon for _ in range(n_lat)]
elev_ft = [100.0] * (n_lat * n_lon)  # flat desert, ~100ft

avoid = ["P", "R"]
result = _rasterize_airspace(grid, avoid, passable, elev_ft=elev_ft, max_msl_ft=5000, min_agl_ft=200)

# Count blocked cells
blocked = sum(1 for i in range(n_lat) for j in range(n_lon) if not passable[i][j])
print(f"Blocked cells: {blocked} out of {n_lat*n_lon}")

# Check specific point inside R-2307
test_i = int((32.95 - lat0) / dlat)
test_j = int((-114.0 - lon0) / dlon)
print(f"Cell ({test_i},{test_j}) at {grid.idx_to_latlon(test_i, test_j)}: passable={passable[test_i][test_j]}")

conn.close()
