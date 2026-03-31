import sqlite3
conn = sqlite3.connect('mvp_backend/airspace_data/airspace.sqlite')

# Check for Reno airspace
rows = conn.execute(
    "SELECT ident, name, class, lower_alt, lower_code, upper_alt, upper_code "
    "FROM airspace WHERE name LIKE '%RENO%' OR ident LIKE '%RENO%'"
).fetchall()
print("=== Reno airspace entries ===")
for r in rows:
    print(r)

# Also check Class C entries in Nevada area (Reno ~39.5N, 119.8W)
rows2 = conn.execute(
    "SELECT ident, name, class, lower_alt, lower_code, upper_alt, upper_code, "
    "min_lat, max_lat, min_lon, max_lon "
    "FROM airspace WHERE class='C' AND min_lat < 40 AND max_lat > 39 "
    "AND min_lon < -119 AND max_lon > -120"
).fetchall()
print("\n=== Class C near Reno area ===")
for r in rows2:
    print(r)

# Check all Class B/C in the general Nevada area
rows3 = conn.execute(
    "SELECT ident, name, class, lower_alt, lower_code, upper_alt, upper_code, "
    "min_lat, max_lat, min_lon, max_lon "
    "FROM airspace WHERE class IN ('B','C') AND min_lat < 41 AND max_lat > 38 "
    "AND min_lon < -118 AND max_lon > -121"
).fetchall()
print("\n=== Class B/C in wider Reno area ===")
for r in rows3:
    print(r)

conn.close()
