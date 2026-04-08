import sqlite3
conn = sqlite3.connect('mvp_backend/airspace_data/airspace.sqlite')

print('Classes:', conn.execute('SELECT DISTINCT class FROM airspace').fetchall())

# Check R areas near Camp Pendleton (33.2N, -117.4W area)
rows = conn.execute(
    "SELECT name, class, lower_alt, lower_code, upper_alt, upper_code, "
    "min_lat, max_lat, min_lon, max_lon "
    "FROM airspace WHERE class IN ('R','P') "
    "AND min_lat < 33.6 AND max_lat > 33.0 "
    "AND min_lon < -117.0 AND max_lon > -117.8 "
    "ORDER BY class, name"
).fetchall()

print(f"\n=== R/P areas near Camp Pendleton ({len(rows)} entries) ===")
for r in rows:
    name, cls, la, lc, ua, uc, mn_lat, mx_lat, mn_lon, mx_lon = r
    floor_str = f"{la} {lc}" if la is not None else "NULL"
    ceil_str = f"{ua} {uc}" if ua is not None else "NULL"
    print(f"  [{cls}] {name:30s}  Floor: {floor_str:15s}  Ceil: {ceil_str:15s}  "
          f"Lat: {mn_lat:.2f}-{mx_lat:.2f}  Lon: {mn_lon:.2f}-{mx_lon:.2f}")

conn.close()
