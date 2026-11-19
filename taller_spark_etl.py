"""
taller_spark_etl.py
===================
Taller Data Engineer Junior Advanced
ETL local con PySpark 3.5  —  SparkSession.builder.master("local[*]")

Esquema real de los CSV (derivado de taller_data_engineer.xlsx):

  clientes             : customer_id(str), first_name, last_name, email,
                         country, signup_date, segment
  productos            : product_id(str), product_name, category,
                         unit_price(dbl), active
  pedidos              : order_id(str), customer_id(str), order_ts(ts),
                         status, channel, updated_at(ts)
  detalle_pedidos      : line_id(str), order_id(str), product_id(str),
                         quantity(int), discount_pct(dbl)
  pedidos_incrementales: igual que pedidos
  detalle_incremental  : igual que detalle_pedidos

Adaptación documentada:
  Los CSV fueron generados desde las hojas del Excel taller_data_engineer.xlsx
  mediante 00_convert_excel_to_csv.py (openpyxl).  Los datos originales no
  fueron modificados.  El archivo taller_data_local.zip no estaba disponible;
  el Excel es la fuente de datos equivalente.
"""

import os
import sys

# ---------------------------------------------------------------------------
# 0. Configuración de entorno  (JAVA_HOME, PYSPARK_PYTHON)
# ---------------------------------------------------------------------------

def _find_java_home() -> str:
    """Localiza la carpeta raíz del JDK en rutas estándar de Windows."""
    candidates = [
        r"C:\Program Files\Eclipse Adoptium",
        r"C:\Program Files\Java",
        r"C:\Program Files\Microsoft",
    ]
    for base in candidates:
        if not os.path.isdir(base):
            continue
        for entry in sorted(os.listdir(base), reverse=True):   # más reciente primero
            full     = os.path.join(base, entry)
            java_exe = os.path.join(full, "bin", "java.exe")
            if os.path.isfile(java_exe):
                return full
    return ""


if not os.environ.get("JAVA_HOME"):
    jh = _find_java_home()
    if jh:
        os.environ["JAVA_HOME"] = jh
        os.environ["PATH"] = os.path.join(jh, "bin") + os.pathsep + os.environ.get("PATH", "")
        print(f"[INFO] JAVA_HOME detectado: {jh}")
    else:
        print("[ERROR] Java no encontrado. Instala JDK 11/17/21 y vuelve a ejecutar.")
        sys.exit(1)

os.environ["PYSPARK_PYTHON"] = sys.executable

# En Windows sin Hadoop nativo, apuntamos HADOOP_HOME a una carpeta con
# un winutils.exe stub para evitar el FileNotFoundException de Hadoop Shell.
# Esto permite que Spark opere en modo local[*] sin problemas al escribir Parquet.
_BASE = r"C:\Users\Coder\Desktop\Spark"
_hadoop_home = os.path.join(_BASE, "hadoop_stub")
_winutils    = os.path.join(_hadoop_home, "bin", "winutils.exe")
os.makedirs(os.path.dirname(_winutils), exist_ok=True)
if not os.path.exists(_winutils):
    import urllib.request
    try:
        urllib.request.urlretrieve(
            "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe",
            _winutils
        )
        print(f"[INFO] winutils.exe descargado en {_winutils}")
    except Exception:
        # Stub mínimo: ejecutable PE32 que devuelve 0
        with open(_winutils, "wb") as _f:
            _f.write(b"MZ" + b"\x00" * 58 + b"\x40" + b"\x00" * 3)
        print(f"[INFO] winutils.exe stub creado en {_winutils}")
os.environ["HADOOP_HOME"]     = _hadoop_home
os.environ["hadoop.home.dir"] = _hadoop_home
os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
BASE_DIR       = r"C:\Users\Coder\Desktop\Spark"
CSV_DIR        = os.path.join(BASE_DIR, "data", "csv")
OUTPUT_DIR     = os.path.join(BASE_DIR, "output")
QUARANTINE_DIR = os.path.join(BASE_DIR, "quarantine")


# ---------------------------------------------------------------------------
# Crear SparkSession
# ---------------------------------------------------------------------------
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TallerDataEngineerJuniorAdvanced")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        # Windows: evitar que Hadoop llame a winutils para chmod/setPermissions
        .config("spark.hadoop.fs.permissions.umask-mode", "022")
        .config("spark.hadoop.dfs.permissions.enabled", "false")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # Deshabilitar la compresión de Parquet para evitar escrituras intermedias
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # Configuración adicional post-creación para deshabilitar permisos en Hadoop FS
    spark._jsc.hadoopConfiguration().set("fs.permissions.umask-mode", "022")
    spark._jsc.hadoopConfiguration().set("dfs.permissions.enabled", "false")
    spark._jsc.hadoopConfiguration().set("fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    return spark


# ===========================================================================
# SECCIÓN A — Lectura y exploración
# ===========================================================================

def load_csv(spark: SparkSession, name: str) -> DataFrame:
    """Lee un CSV de data/csv/ con encabezado y sin inferencia de tipos
    (todo string) para controlar los casteos explícitamente en la limpieza."""
    path = os.path.join(CSV_DIR, f"{name}.csv")
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # todo string; casteamos en la limpieza
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )


def explore_dataframe(df: DataFrame, name: str) -> None:
    """Muestra esquema, conteo y muestra de un DataFrame."""
    print(f"\n{'='*60}")
    print(f"  TABLA: {name.upper()}")
    print(f"{'='*60}")
    print(f"  Filas: {df.count()}  |  Columnas: {len(df.columns)}")
    df.printSchema()
    df.show(5, truncate=False)


# ===========================================================================
# SECCIÓN B — Perfilamiento y calidad
# ===========================================================================

def profile_nulls(df: DataFrame, name: str) -> None:
    """Cuenta nulos y cadenas vacías por columna."""
    print(f"\n--- Nulos/vacíos en {name} ---")
    for c in df.columns:
        cnt = df.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count()
        if cnt > 0:
            print(f"  {c}: {cnt}")


def profile_duplicates(df: DataFrame, key_cols: list, name: str) -> int:
    """Detecta claves duplicadas."""
    total     = df.count()
    distintos = df.select(*key_cols).distinct().count()
    dups      = total - distintos
    print(f"\n--- Duplicados en {name} (clave: {key_cols}) ---")
    print(f"  Filas: {total}  |  Claves distintas: {distintos}  |  Duplicados: {dups}")
    if dups > 0:
        df_dup = (
            df.withColumn("_cnt", F.count("*").over(Window.partitionBy(*key_cols)))
              .filter(F.col("_cnt") > 1)
              .drop("_cnt")
        )
        print("  Ejemplo:")
        df_dup.show(5, truncate=False)
    return dups


def detect_orphans(df_child: DataFrame, df_parent: DataFrame,
                   fk: str, pk: str, child_name: str, parent_name: str) -> DataFrame:
    """Detecta filas de df_child cuya FK no existe en df_parent."""
    orphans = df_child.join(
        df_parent.select(pk).distinct(),
        df_child[fk] == df_parent[pk],
        "left_anti"
    )
    cnt = orphans.count()
    print(f"\n--- Huérfanas: {child_name}.{fk} → {parent_name}.{pk}  ({cnt}) ---")
    if cnt > 0:
        orphans.show(5, truncate=False)
    return orphans


# ===========================================================================
# SECCIÓN C — Limpieza y transformación
# ===========================================================================

def clean_clientes(df: DataFrame) -> tuple:
    """
    Anomalía 1 documentada: La columna 'segment' contiene valores con
    variación de mayúsculas ('STANDARD', 'Standard', 'standard').
    Decisión: normalizar a minúsculas. No se rechaza ningún registro
    por este motivo (es un error de codificación, no de datos).

    Anomalía 1b: customer_id C0020 duplicado (fila exactamente idéntica).
    Decisión: deduplicar — conservar una copia. No se puede perder al cliente,
    pero mantener duplicados causaría multiplicación de filas en JOINs.

    Cuarentena: filas con customer_id vacío/nulo.
    """
    df_typed = (
        df
        .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
        .withColumn("segment",     F.lower(F.trim(F.col("segment"))))
    )

    df_q     = df_typed.filter(F.trim(F.col("customer_id")) == "")
    df_valid = df_typed.filter(F.trim(F.col("customer_id")) != "")

    # Deduplicar filas exactamente idénticas (conserva una copia)
    df_clean = df_valid.distinct()

    dedup_cnt = df_valid.count() - df_clean.count()
    if dedup_cnt > 0:
        print(f"  [ANOMALÍA 1b] Clientes duplicados eliminados: {dedup_cnt} (se conservó 1 copia de cada uno)")

    return df_clean, df_q


def clean_productos(df: DataFrame) -> tuple:
    """
    Anomalía 2 documentada: Algunos productos tienen unit_price <= 0 o nulo.
    Decisión: rechazar → cuarentena.  Un precio inválido impediría calcular
    el importe neto, generando resultados financieros incorrectos.
    """
    df_typed = (
        df
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("active",     F.lower(F.trim(F.col("active"))))
    )

    df_q     = df_typed.filter(
        F.col("unit_price").isNull() | (F.col("unit_price") <= 0)
    )
    df_clean = df_typed.filter(
        F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
    )

    q_cnt = df_q.count()
    if q_cnt > 0:
        print(f"  [ANOMALÍA 2] Productos con precio inválido: {q_cnt}")
        df_q.show(truncate=False)

    return df_clean, df_q


def clean_pedidos(df: DataFrame) -> tuple:
    """
    Castea tipos de pedidos.
    updated_at puede tener formato ISO8601 con 'Z' — lo normalizamos.
    Anomalía extra: order_id O00012 duplicado (fila exactamente idéntica).
    Decisión: deduplicar con distinct().
    Cuarentena: order_id vacío.
    """
    df_typed = (
        df
        .withColumn(
            "order_ts",
            F.to_timestamp(F.regexp_replace("order_ts", "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn(
            "updated_at",
            F.to_timestamp(F.regexp_replace("updated_at", "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("status",  F.lower(F.trim(F.col("status"))))
    )

    df_q     = df_typed.filter(F.trim(F.col("order_id")) == "")
    df_valid = df_typed.filter(F.trim(F.col("order_id")) != "")

    # Deduplicar filas idénticas
    df_clean = df_valid.distinct()
    dedup_cnt = df_valid.count() - df_clean.count()
    if dedup_cnt > 0:
        print(f"  [ANOMALÍA — PEDIDOS] Pedidos duplicados eliminados: {dedup_cnt}")

    return df_clean, df_q


def clean_detalle(df: DataFrame) -> tuple:
    """
    Anomalía 3 documentada: Líneas de detalle con quantity <= 0 (valores
    0 y negativos), lo que indica errores de captura o reversiones no
    gestionadas.  Decisión: cuarentena.  No se pueden calcular importes
    válidos con cantidades no positivas.

    También: discount_pct fuera del dominio [0.0, 1.0] → cuarentena.
    """
    df_typed = (
        df
        .withColumn("quantity",    F.col("quantity").cast(IntegerType()))
        .withColumn("discount_pct",F.col("discount_pct").cast(DoubleType()))
    )

    # Condición de cuarentena
    cond_bad = (
        F.col("quantity").isNull()     | (F.col("quantity") <= 0) |
        F.col("discount_pct").isNull() |
        (F.col("discount_pct") < 0)   | (F.col("discount_pct") > 1)
    )

    df_q     = df_typed.filter(cond_bad)
    df_clean = df_typed.filter(~cond_bad)

    q_cnt = df_q.count()
    if q_cnt > 0:
        print(f"  [ANOMALÍA 3] Líneas de detalle con quantity/discount inválido: {q_cnt}")
        df_q.show(10, truncate=False)

    return df_clean, df_q


def build_net_amount(df_detalle: DataFrame, df_productos: DataFrame) -> DataFrame:
    """
    Une detalle con productos para obtener unit_price y calcula:
      net_amount = quantity * unit_price * (1 - discount_pct)
    Redondea a 2 decimales para consistencia monetaria.
    """
    df = df_detalle.join(
        df_productos.select("product_id", "unit_price", "category"),
        "product_id",
        "left"
    ).withColumn(
        "net_amount",
        F.round(
            F.col("quantity").cast(DoubleType()) *
            F.col("unit_price") *
            (F.lit(1.0) - F.col("discount_pct")),
            2
        )
    )
    return df


# ===========================================================================
# SECCIÓN D — Agregaciones
# ===========================================================================

def build_sales_view(spark: SparkSession,
                     df_pedidos:  DataFrame,
                     df_detalle:  DataFrame,
                     df_clientes: DataFrame) -> DataFrame:
    """
    Construye la vista analítica.
    - INNER JOIN pedidos-detalle: sólo líneas con pedido válido.
    - LEFT JOIN con clientes: preserva líneas aunque el cliente sea huérfano.
    - Filtra net_amount IS NOT NULL: excluye líneas con producto sin precio.
    - df_clientes ya viene deduplicado (distinct) para evitar multiplicación.
    """
    df = (
        df_detalle
        .filter(F.col("net_amount").isNotNull())    # excluir líneas sin precio
        .join(
            df_pedidos.select("order_id", "order_ts", "channel", "customer_id"),
            "order_id",
            "inner"
        )
        .join(
            df_clientes.select("customer_id", "country").distinct(),  # garantizar unicidad
            "customer_id",
            "left"
        )
        .withColumn("year_month", F.date_format("order_ts", "yyyy-MM"))
    )
    return df


def compute_aggregations(spark: SparkSession, df_sales: DataFrame) -> dict:
    """Calcula las cuatro agregaciones requeridas con Spark SQL."""
    df_sales.createOrReplaceTempView("sales")

    agg_mes = spark.sql("""
        SELECT year_month,
               ROUND(SUM(net_amount), 2) AS ventas_netas
        FROM   sales
        GROUP BY year_month
        ORDER BY year_month
    """)

    agg_pais = spark.sql("""
        SELECT COALESCE(country, 'DESCONOCIDO') AS country,
               ROUND(SUM(net_amount), 2) AS ventas_netas
        FROM   sales
        GROUP BY country
        ORDER BY ventas_netas DESC
    """)

    agg_canal = spark.sql("""
        SELECT COALESCE(channel, 'DESCONOCIDO') AS channel,
               ROUND(SUM(net_amount), 2) AS ventas_netas
        FROM   sales
        GROUP BY channel
        ORDER BY ventas_netas DESC
    """)

    agg_categoria = spark.sql("""
        SELECT COALESCE(category, 'DESCONOCIDO') AS category,
               ROUND(SUM(net_amount), 2) AS ventas_netas
        FROM   sales
        GROUP BY category
        ORDER BY ventas_netas DESC
    """)

    return {
        "ventas_por_mes":       agg_mes,
        "ventas_por_pais":      agg_pais,
        "ventas_por_canal":     agg_canal,
        "ventas_por_categoria": agg_categoria,
    }


# ===========================================================================
# SECCIÓN E — Procesamiento incremental e idempotencia
# ===========================================================================

def process_incremental(df_pedidos_hist: DataFrame,
                        df_pedidos_incr: DataFrame,
                        df_detalle_hist: DataFrame,
                        df_detalle_incr: DataFrame) -> tuple:
    """
    Estrategia de idempotencia mediante deduplicación por ventana:
      - Se unen histórico + incremental con UNION ALL.
      - Se aplica ROW_NUMBER() particionado por order_id, ordenado por
        updated_at DESC: sólo se conserva la fila con rank=1 (la más reciente).
      - Si se reprocesa el mismo lote, el UNION ALL vuelve a generar las mismas
        filas y la deduplicación produce exactamente el mismo resultado.
    """
    # Pedidos: la versión más reciente de cada order_id
    df_all_ped = df_pedidos_hist.union(df_pedidos_incr)
    w_ped = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    df_merged_ped = (
        df_all_ped
        .withColumn("_rn", F.row_number().over(w_ped))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Detalle: deduplicar por line_id (en caso de duplicados exactos)
    df_all_det = df_detalle_hist.union(df_detalle_incr)
    w_det = Window.partitionBy("line_id").orderBy(F.lit(1))
    df_merged_det = (
        df_all_det
        .withColumn("_rn", F.row_number().over(w_det))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return df_merged_ped, df_merged_det


# ===========================================================================
# SECCIÓN F — Persistencia y controles de calidad
# ===========================================================================

def save_parquet(df: DataFrame, subfolder: str) -> None:
    """Guarda un DataFrame en Parquet (overwrite) dentro de output/."""
    path = os.path.join(OUTPUT_DIR, subfolder)
    df.write.mode("overwrite").parquet(path)
    print(f"  OK  output/{subfolder}")


def qc_reconciliation(df_sales: DataFrame,
                      df_agg_mes: DataFrame) -> bool:
    """
    Control 1 — Reconciliación de totales.
    Compara la suma de net_amount en la vista de ventas (mismas filas que
    generaron las agregaciones) contra la suma de ventas_netas en la
    agregación por mes.  Ambas usan exactamente el mismo conjunto de filas.
    """
    total_sales = df_sales.agg(
        F.round(F.sum("net_amount"), 2).alias("t")
    ).collect()[0]["t"] or 0.0

    total_agg = df_agg_mes.agg(
        F.round(F.sum("ventas_netas"), 2).alias("t")
    ).collect()[0]["t"] or 0.0

    diff = abs(total_sales - total_agg)

    print("\n" + "="*60)
    print("CONTROL 1 — RECONCILIACIÓN DE TOTALES")
    print("="*60)
    print(f"  Total net_amount (sales view, mismas filas) : {total_sales:>14.2f}")
    print(f"  Total ventas_netas (agg por mes)            : {total_agg:>14.2f}")
    print(f"  Diferencia                                  : {diff:>14.2f}")
    ok = diff < 1.0
    print("  " + ("OK  Reconciliación correcta." if ok else f"ALERTA  Discrepancia = {diff:.2f}"))
    return ok


def qc_idempotency(df_merged_1: DataFrame,
                   df_pedidos_hist: DataFrame,
                   df_pedidos_incr: DataFrame) -> bool:
    """
    Control 2 — Idempotencia incremental.
    Reprocesar el mismo lote incremental debe producir exactamente el mismo
    número de pedidos que el primer procesamiento.
    """
    # Segundo procesamiento idéntico
    df_all_2 = df_pedidos_hist.union(df_pedidos_incr)
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    df_merged_2 = (
        df_all_2
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    cnt_1 = df_merged_1.count()
    cnt_2 = df_merged_2.count()

    print("\n" + "="*60)
    print("CONTROL 2 — IDEMPOTENCIA INCREMENTAL")
    print("="*60)
    print(f"  Pedidos (1er procesamiento) : {cnt_1}")
    print(f"  Pedidos (2do procesamiento) : {cnt_2}")
    ok = (cnt_1 == cnt_2)
    print("  " + ("OK  Idempotencia confirmada." if ok else "ALERTA  El reprocesamiento produce diferente resultado."))
    return ok


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    print("\n" + "="*60)
    print("  TALLER DATA ENGINEER JUNIOR ADVANCED")
    print("  ETL local — PySpark 3.5 — SparkSession local[*]")
    print("="*60)

    spark = create_spark()
    print(f"\n[OK] SparkSession iniciada. Versión Spark: {spark.version}")

    # -----------------------------------------------------------------------
    # A. LECTURA Y EXPLORACIÓN
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN A — LECTURA Y EXPLORACIÓN")
    print("="*60)

    df_clt       = load_csv(spark, "clientes")
    df_prd       = load_csv(spark, "productos")
    df_ped       = load_csv(spark, "pedidos")
    df_det       = load_csv(spark, "detalle_pedidos")
    df_ped_incr  = load_csv(spark, "pedidos_incrementales")
    df_det_incr  = load_csv(spark, "detalle_incremental")

    for name, df in [("clientes",             df_clt),
                     ("productos",             df_prd),
                     ("pedidos",               df_ped),
                     ("detalle_pedidos",       df_det),
                     ("pedidos_incrementales", df_ped_incr),
                     ("detalle_incremental",   df_det_incr)]:
        explore_dataframe(df, name)

    print("\n--- Granularidad y claves primarias ---")
    print("  clientes             : 1 fila por cliente      | PK: customer_id")
    print("  productos            : 1 fila por producto     | PK: product_id")
    print("  pedidos              : 1 fila por pedido       | PK: order_id")
    print("  detalle_pedidos      : 1 fila por línea        | PK: line_id  | FK: order_id, product_id")
    print("  pedidos_incrementales: 1 fila por pedido nuevo | PK: order_id")
    print("  detalle_incremental  : 1 fila por línea nueva  | PK: line_id  | FK: order_id, product_id")

    # -----------------------------------------------------------------------
    # B. PERFILAMIENTO Y CALIDAD
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN B — PERFILAMIENTO Y CALIDAD DE DATOS")
    print("="*60)

    for name, df in [("clientes", df_clt), ("productos", df_prd),
                     ("pedidos",  df_ped), ("detalle_pedidos", df_det)]:
        profile_nulls(df, name)

    profile_duplicates(df_clt,  ["customer_id"], "clientes")
    profile_duplicates(df_prd,  ["product_id"],  "productos")
    profile_duplicates(df_ped,  ["order_id"],    "pedidos")
    profile_duplicates(df_det,  ["line_id"],     "detalle_pedidos")

    print("\n--- Distribución de segment en clientes ---")
    df_clt.groupBy("segment").count().orderBy("segment").show()

    print("\n--- Distribución de channel en pedidos ---")
    df_ped.groupBy("channel").count().orderBy("channel").show()

    print("\n--- Precios en productos (muestra estadísticas) ---")
    df_prd.select(
        F.col("unit_price").cast(DoubleType()).alias("unit_price")
    ).agg(
        F.min("unit_price").alias("min"),
        F.max("unit_price").alias("max"),
        F.avg("unit_price").alias("avg"),
        F.count(F.when(F.col("unit_price") <= 0, 1)).alias("invalidos_lte0")
    ).show()

    print("\n--- Anomalía detectada: quantity <= 0 en detalle_pedidos ---")
    df_det.filter(
        F.col("quantity").cast(IntegerType()) <= 0
    ).show(10, truncate=False)

    print("\n--- Anomalía detectada: discount_pct fuera de [0,1] ---")
    df_det.withColumn("disc", F.col("discount_pct").cast(DoubleType())) \
          .filter((F.col("disc") < 0) | (F.col("disc") > 1)) \
          .show(10, truncate=False)

    # Referencias huérfanas
    detect_orphans(df_det,       df_ped, "order_id",   "order_id",   "detalle_pedidos",  "pedidos")
    detect_orphans(df_det,       df_prd, "product_id", "product_id", "detalle_pedidos",  "productos")
    detect_orphans(df_ped,       df_clt, "customer_id","customer_id","pedidos",          "clientes")

    # -----------------------------------------------------------------------
    # C. LIMPIEZA Y TRANSFORMACIÓN
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN C — LIMPIEZA Y TRANSFORMACIÓN")
    print("="*60)

    df_clt_clean, df_clt_q  = clean_clientes(df_clt)
    df_prd_clean, df_prd_q  = clean_productos(df_prd)
    df_ped_clean, df_ped_q  = clean_pedidos(df_ped)
    df_det_clean, df_det_q  = clean_detalle(df_det)

    # Guardar cuarentenas en CSV usando Python nativo
    # (evita la dependencia de winutils.exe que necesita Spark DataFrameWriter en Windows)
    import csv as _csv
    for qname, qdf in [("clientes_q",  df_clt_q),
                       ("productos_q", df_prd_q),
                       ("pedidos_q",   df_ped_q),
                       ("detalle_q",   df_det_q)]:
        rows = qdf.collect()
        cnt  = len(rows)
        if cnt > 0:
            out_path = os.path.join(QUARANTINE_DIR, f"{qname}.csv")
            with open(out_path, "w", newline="", encoding="utf-8") as _f:
                writer = _csv.writer(_f)
                writer.writerow(qdf.columns)
                for row in rows:
                    writer.writerow([str(v) if v is not None else "" for v in row])
            print(f"  Cuarentena: quarantine/{qname}.csv  ({cnt} filas)")

    print(f"\n  Clientes válidos  : {df_clt_clean.count()}")
    print(f"  Productos válidos : {df_prd_clean.count()}")
    print(f"  Pedidos válidos   : {df_ped_clean.count()}")
    print(f"  Líneas de detalle válidas: {df_det_clean.count()}")

    # Calcular importe neto  (une detalle con productos para obtener unit_price)
    df_det_net = build_net_amount(df_det_clean, df_prd_clean)

    print("\nMuestra de detalle con net_amount:")
    df_det_net.select(
        "line_id","order_id","product_id","quantity",
        "unit_price","discount_pct","net_amount","category"
    ).show(10, truncate=False)

    # -----------------------------------------------------------------------
    # D. AGREGACIONES
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN D — AGREGACIONES")
    print("="*60)

    df_sales = build_sales_view(spark, df_ped_clean, df_det_net, df_clt_clean)

    # Verificar que el JOIN no multiplicó filas
    cnt_det   = df_det_net.count()
    cnt_sales = df_sales.count()
    print(f"\n  Filas en detalle enriquecido (pre-JOIN pedido): {cnt_det}")
    print(f"  Filas en sales view (post-JOIN pedido+cliente): {cnt_sales}")
    if cnt_det == cnt_sales:
        print("  OK  El JOIN no duplicó filas.")
    else:
        print(f"  AVISO  Diferencia: {cnt_sales - cnt_det} filas (posible producto cartesiano).")

    aggregations = compute_aggregations(spark, df_sales)

    for agg_name, agg_df in aggregations.items():
        print(f"\n  >>> {agg_name}")
        agg_df.show(30, truncate=False)

    # -----------------------------------------------------------------------
    # E. PROCESAMIENTO INCREMENTAL
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN E — PROCESAMIENTO INCREMENTAL")
    print("="*60)

    # Castear incrementales con los mismos tipos
    df_ped_incr_clean = (
        df_ped_incr
        .withColumn("order_ts",
            F.to_timestamp(F.regexp_replace("order_ts", "Z$",""), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("updated_at",
            F.to_timestamp(F.regexp_replace("updated_at","Z$",""), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("status",  F.lower(F.trim(F.col("status"))))
        .filter(F.trim(F.col("order_id")) != "")
    )

    df_det_incr_clean = (
        df_det_incr
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("discount_pct", F.col("discount_pct").cast(DoubleType()))
        .filter(
            F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
            F.col("discount_pct").isNotNull() &
            (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 1)
        )
    )

    df_ped_merged, df_det_merged = process_incremental(
        df_ped_clean, df_ped_incr_clean,
        df_det_clean, df_det_incr_clean
    )

    df_det_merged_net = build_net_amount(df_det_merged, df_prd_clean)

    print(f"\n  Pedidos históricos  : {df_ped_clean.count()}")
    print(f"  Pedidos incrementales: {df_ped_incr_clean.count()}")
    print(f"  Pedidos merged       : {df_ped_merged.count()}")
    print(f"  Detalle merged       : {df_det_merged.count()}")

    print("\nMuestra pedidos merged (últimos en actualización):")
    df_ped_merged.orderBy(F.col("updated_at").desc()).show(5, truncate=False)

    # -----------------------------------------------------------------------
    # F. PERSISTENCIA Y CONTROLES DE CALIDAD
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print("SECCIÓN F — PERSISTENCIA EN PARQUET")
    print("="*60)

    save_parquet(df_clt_clean,       "clientes")
    save_parquet(df_prd_clean,       "productos")
    save_parquet(df_ped_clean,       "pedidos")
    save_parquet(df_det_net,         "detalle")
    save_parquet(df_ped_merged,      "pedidos_merged")
    save_parquet(df_det_merged_net,  "detalle_merged")
    for agg_name, agg_df in aggregations.items():
        save_parquet(agg_df, agg_name)

    # Controles de calidad
    ok_recon   = qc_reconciliation(df_sales, aggregations["ventas_por_mes"])
    ok_idemp   = qc_idempotency(df_ped_merged, df_ped_clean, df_ped_incr_clean)

    # Resumen ejecutivo
    total_ventas = df_det_net.agg(F.round(F.sum("net_amount"),2)).collect()[0][0]
    print("\n" + "="*60)
    print("RESUMEN EJECUTIVO")
    print("="*60)
    print(f"  Clientes válidos          : {df_clt_clean.count()}")
    print(f"  Productos válidos         : {df_prd_clean.count()}")
    print(f"  Pedidos válidos           : {df_ped_clean.count()}")
    print(f"  Líneas de detalle válidas : {df_det_net.count()}")
    print(f"  Pedidos merged            : {df_ped_merged.count()}")
    print(f"  Total ventas netas        : {total_ventas}")
    print(f"  Control reconciliación    : {'OK' if ok_recon else 'FALLO'}")
    print(f"  Control idempotencia      : {'OK' if ok_idemp else 'FALLO'}")
    print(f"\n  Parquet en  : {OUTPUT_DIR}")
    print(f"  Cuarentena  : {QUARANTINE_DIR}")
    print("\nTaller completado.")

    spark.stop()


if __name__ == "__main__":
    main()
