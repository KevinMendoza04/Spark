"""
00_convert_excel_to_csv.py
==========================
Convierte las seis hojas del Excel taller_data_engineer.xlsx a archivos CSV
en la carpeta data/csv/.  Este paso es necesario porque PySpark lee CSV/Parquet
nativamente pero no archivos .xlsx.

Usa ÚNICAMENTE openpyxl (sin pandas) para evitar bloqueos de WDAC sobre DLLs.
Los datos originales del Excel NO se modifican.
"""

import os
import csv
import openpyxl

EXCEL_PATH = r"C:\Users\Coder\Desktop\Spark\taller_data_engineer.xlsx"
CSV_DIR    = r"C:\Users\Coder\Desktop\Spark\data\csv"

SHEETS = [
    "clientes",
    "productos",
    "pedidos",
    "detalle_pedidos",
    "pedidos_incrementales",
    "detalle_incremental",
]


def cell_to_str(value) -> str:
    """Convierte cualquier valor de celda a cadena limpia."""
    if value is None:
        return ""
    # Fechas y datetimes: convertir a ISO
    import datetime
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    return str(value)


def convert_excel_to_csv():
    print("=" * 60)
    print("CONVERSIÓN EXCEL → CSV (openpyxl)")
    print("=" * 60)

    wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, data_only=True)

    for sheet in SHEETS:
        if sheet not in wb.sheetnames:
            print(f"  ⚠  Hoja '{sheet}' no encontrada en el Excel. Saltando.")
            continue

        ws       = wb[sheet]
        out_path = os.path.join(CSV_DIR, f"{sheet}.csv")
        rows_written = 0

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            for row in ws.iter_rows(values_only=True):
                writer.writerow([cell_to_str(c) for c in row])
                rows_written += 1

        # La primera fila es el encabezado, así que filas de datos = rows_written - 1
        print(f"  OK  {sheet:30s}  {rows_written - 1:>5} filas de datos  ->  {out_path}")

    wb.close()
    print("\nConversion completada. Los CSV estan en:", CSV_DIR)


if __name__ == "__main__":
    convert_excel_to_csv()
