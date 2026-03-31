import sqlite3
conn = sqlite3.connect('mvp_backend/airspace_data/airspace.sqlite')

# Check all R and P areas in Nevada (roughly 36-42N, 115-120W)
rows = conn.execute(
    "SELECT name, class, lower_alt, lower_code, upper_alt, upper_code, "
    "min_lat, max_lat, min_lon, max_lon "
    "FROM airspace WHERE class IN ('R','P','MOA') "
    "AND min_lat < 42 AND max_lat > 36 "
    "AND min_lon < -115 AND max_lon > -120 "
    "ORDER BY class, name"
).fetchall()

print(f"=== Restricted/Prohibited/MOA in Nevada area ({len(rows)} entries) ===")
for r in rows:
    name, cls, la, lc, ua, uc, mn_lat, mx_lat, mn_lon, mx_lon = r
    floor_str = f"{la} {lc}" if la is not None else "NULL"
    ceil_str = f"{ua} {uc}" if ua is not None else "NULL"
    print(f"  [{cls}] {name:30s}  Floor: {floor_str:15s}  Ceil: {ceil_str:15s}  "
          f"Lat: {mn_lat:.2f}-{mx_lat:.2f}  Lon: {mn_lon:.2f}-{mx_lon:.2f}")

conn.close()
