"""
Verificación del entorno Python (geoenv / global-env).
Ejecutar desde Positron o: python src/test_env.py
"""

import sys
import importlib

# ── 1. Info del intérprete ─────────────────────────────────────────────────
print(f"Python : {sys.version}")
print(f"Ruta   : {sys.executable}")
print()

# ── 2. Módulos a verificar ─────────────────────────────────────────────────
modulos = {
    "sqlite3"   : "stdlib — requerido por Positron/ipykernel",
    "pandas"    : "manipulación de datos",
    "numpy"     : "cómputo numérico",
    "requests"  : "HTTP / descarga de datos",
    "geopandas" : "datos geoespaciales vectoriales",
}

ok, fallo = [], []

for mod, descripcion in modulos.items():
    try:
        importlib.import_module(mod)
        ok.append((mod, descripcion))
    except ModuleNotFoundError:
        fallo.append((mod, descripcion))

print("OK:")
for mod, desc in ok:
    print(f"  ✓  {mod:<12} {desc}")

if fallo:
    print("\nFALTA:")
    for mod, desc in fallo:
        print(f"  ✗  {mod:<12} {desc}")
else:
    print("\nTodos los módulos disponibles.")

# ── 3. Test funcional sqlite3 ──────────────────────────────────────────────
print("\n── sqlite3 ───────────────────────────────────────────────────────────")
import sqlite3

con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE t (id INTEGER, valor REAL)")
con.execute("INSERT INTO t VALUES (1, 3.14)")
row = con.execute("SELECT * FROM t").fetchone()
con.close()
assert row == (1, 3.14), "Error en sqlite3"
print(f"  in-memory DB → {row}  OK")

# ── 4. Test funcional pandas ───────────────────────────────────────────────
print("\n── pandas ────────────────────────────────────────────────────────────")
import pandas as pd

df = pd.DataFrame({"estacion": ["A", "B", "C"], "temp_c": [12.3, 8.7, 15.1]})
media = df["temp_c"].mean()
print(f"  DataFrame OK — temp media: {media:.2f} °C")

print("\nEntorno listo.")
