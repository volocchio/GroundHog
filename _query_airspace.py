import sqlite3
c = sqlite3.connect('mvp_backend/airspace_data/airspace.sqlite')

print('=== DISTINCT class VALUES ===')
for r in c.execute('SELECT class, COUNT(*) FROM airspace GROUP BY class ORDER BY class'):
    print(f'  {r[0]:>6}: {r[1]}')

print('\n=== DISTINCT local_type VALUES ===')
for r in c.execute('SELECT local_type, COUNT(*) FROM airspace GROUP BY local_type ORDER BY local_type'):
    print(f'  {repr(r[0]):>20}: {r[1]}')

print('\n=== GRAND CANYON / SFRA / TFR ===')
for r in c.execute("SELECT ident, name, class, local_type, lower_alt, lower_code, upper_alt, upper_code FROM airspace WHERE name LIKE '%GRAND CANYON%' OR name LIKE '%SFRA%' OR local_type LIKE '%SFRA%' OR local_type LIKE '%TFR%' OR local_type LIKE '%SPECIAL%'"):
    print(f'  {r}')

print('\n=== SAMPLE: class=R, local_type != empty ===')
for r in c.execute("SELECT ident, name, class, local_type FROM airspace WHERE class='R' AND local_type IS NOT NULL AND local_type != '' LIMIT 10"):
    print(f'  {r}')
