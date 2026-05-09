"""Debug water detection for Lake Pend Oreille - correct N48 coordinates."""
import math, sys
sys.path.insert(0, '/app')
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.planner import GridSpec, _detect_water_grid, _deg_per_km_lat, _deg_per_km_lon

provider = SRTMProvider('/app/mvp_backend/srtm_cache')

# Sandpoint Airport: 48.2997 N, -116.5569 W
pts = [(48.3,-116.55),(48.2,-116.55),(48.1,-116.55),(48.15,-116.4),(48.0,-116.3),(48.25,-116.3),(48.15,-116.65),(48.3,-117.0)]
print("=== SRTM point samples ===")
for lat,lon in pts:
    e = provider.get_many_m([(lat,lon)])[0]
    ft = e*3.28084 if e==e else float('nan')
    print(f"  ({lat:.2f},{lon:.2f}): {e:.1f}m = {ft:.0f}ft")

cell_km=1.0
dlat=cell_km*_deg_per_km_lat()
mid_lat=48.2
dlon=cell_km*_deg_per_km_lon(mid_lat)
lat0,lon0=47.9,-117.1
lat1,lon1=48.6,-115.8
n_lat=int(math.ceil((lat1-lat0)/dlat))+1
n_lon=int(math.ceil((lon1-lon0)/dlon))+1
print(f"\n=== Grid {n_lat}x{n_lon} cells ===")
grid=GridSpec(lat0=lat0,lon0=lon0,n_lat=n_lat,n_lon=n_lon,dlat=dlat,dlon=dlon)
points=[grid.idx_to_latlon(i,j) for i in range(n_lat) for j in range(n_lon)]
elev_m=provider.get_many_m(points)
elev_ft=[m*3.28084 if m==m else float('inf') for m in elev_m]
is_water=_detect_water_grid(n_lat,n_lon,elev_ft)
water_count=sum(1 for i in range(n_lat) for j in range(n_lon) if is_water[i][j])
print(f"Water cells: {water_count}/{n_lat*n_lon} ({100*water_count/(n_lat*n_lon):.1f}%)")

e2d=[[0.0]*n_lon for _ in range(n_lat)]
k=0
for i in range(n_lat):
    for j in range(n_lon):
        e2d[i][j]=elev_ft[k]; k+=1

print("\n=== Water map (W=water .=land) ===")
for i in range(n_lat-1,-1,-1):
    row=''.join('W' if is_water[i][j] else '.' for j in range(n_lon))
    lat=lat0+i*dlat
    print(f"{lat:.2f} {row}")

print("\n=== Transect at lat=48.15 ===")
lat_idx=int(round((48.15-lat0)/dlat))
if 0<=lat_idx<n_lat:
    for j in range(n_lon):
        lon=lon0+j*dlon
        print(f"  lon={lon:.2f}: {e2d[lat_idx][j]:.0f}ft water={is_water[lat_idx][j]}")

