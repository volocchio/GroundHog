from mvp_backend import terrain_intel

for msl in range(5000, 13000, 500):
    v = terrain_intel.check_viability(48.30, -116.56, 46.26, -114.12, msl, 1000)
    viable = v["viable"]
    min_msl = v.get("min_viable_msl")
    print(f"  {msl:>6}' MSL: viable={viable}, min_viable_msl={min_msl}")

# Now check what the planner actually uses
print("\nPlanner check_viability with max_msl_ft=9500, min_agl_ft=1000:")
v = terrain_intel.check_viability(48.30, -116.56, 46.26, -114.12, 9500, 1000)
print(v)

# Check the find_min_viable_msl
print("\nfind_min_viable_msl:")
m = terrain_intel.find_min_viable_msl(48.30, -116.56, 46.26, -114.12, 1000)
print(f"  min viable MSL = {m}")

# Check with lower AGL
print("\nWith 500 ft AGL:")
for msl in [7000, 8000, 9000]:
    v = terrain_intel.check_viability(48.30, -116.56, 46.26, -114.12, msl, 500)
    print(f"  {msl}': viable={v['viable']}")
