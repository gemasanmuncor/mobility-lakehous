import os
import shutil
import glob

SRC_DIR = "data/ine_source"
DST_DIR = "lakehouse/bronze/ine"


def ingest_ine(polygon=None, start_date=None, end_date=None):
    """
    Ingesta datos INE desde CSV locales a Bronze.
    polygon y fechas se aceptan por consistencia con el pipeline,
    aunque aquí no se usen todavía.
    """
    os.makedirs(DST_DIR, exist_ok=True)

    files = glob.glob(os.path.join(SRC_DIR, "*.csv"))
    if not files:
        raise RuntimeError(f"No hay CSV en {SRC_DIR}. Mete ahí los ficheros INE.")

    for f in files:
        dst = os.path.join(DST_DIR, os.path.basename(f))
        shutil.copyfile(f, dst)
        print(f"[INE][OK] {os.path.basename(f)} -> {dst}")

    print(f"[INE] Copiados {len(files)} CSV a Bronze")
