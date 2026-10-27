#!/usr/bin/env python3
"""
============================================================================
Universidad Loyola Andalucía - Ampliación de Bases de Datos (TPI 2)
Tema 5: Big Data y bases de datos a gran escala
Apartado 5.4: Procesamiento con Spark SQL
Apartado 5.5: Integración con sistemas de información empresarial (JDBC)

Autor: Alfonso Nicolás Martínez Márquez
============================================================================
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, avg, sum as spark_sum, count

def main():
    print("\n" + "="*80)
    print(" INICIANDO MOTOR APACHE SPARK CON SPARK SQL (TPI 2 - CAMPUS LOYOLA)")
    print("="*80)

    tiempo_inicio = time.time()

    # 1. Crear la sesión de Spark con el driver JDBC de PostgreSQL
    spark = SparkSession.builder \
        .appName("AnaliticaMovilidadCampusLoyola") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", "/opt/spark/work-dir/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Reducir logs ruidosos para mostrar salida limpia
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] Sesion Spark inicializada correctamente.")
    print(f"      Version de Apache Spark: {spark.version}")

    # 2. Ingesta masiva del Dataset (100.000 registros en CSV)
    csv_path = "/opt/spark/work-dir/datos_movilidad_loyola_100k.csv"
    print(f"[2/5] Cargando dataset masivo desde {csv_path}...")
    
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

    total_registros = df_raw.count()
    print(f"      OK -> {total_registros:,} registros cargados en el DataFrame distribuido.")
    print("\n--- ESQUEMA INFERIDO POR SPARK (CATALYST ENGINE) ---")
    df_raw.printSchema()

    # 3. Registrar vista temporal para permitir sintaxis SQL directa (5.4)
    df_raw.createOrReplaceTempView("movilidad_campus")
    print("\n[3/5] Vista temporal 'movilidad_campus' registrada para consultas SQL.")

    # --------------------------------------------------------------------------
    # CONSULTA SPARK SQL 1: Saturación y retrasos por línea de servicio
    # --------------------------------------------------------------------------
    print("\n" + "-"*80)
    print(" CONSULTA SPARK SQL 1: Analisis de Rendimiento y CO2 por Linea de Transporte")
    print("-"*80)
    sql_lineas = """
        SELECT
            linea_servicio AS linea,
            medio_transporte,
            COUNT(*) AS total_viajes,
            ROUND(AVG(duracion_min), 2) AS duracion_media_min,
            ROUND(AVG(retraso_min), 2) AS retraso_medio_min,
            ROUND(AVG(ocupacion_unidad_pct), 2) AS ocupacion_media_pct,
            ROUND(SUM(co2_emitido_kg), 2) AS co2_total_kg
        FROM movilidad_campus
        GROUP BY linea_servicio, medio_transporte
        ORDER BY total_viajes DESC
    """
    df_lineas = spark.sql(sql_lineas)
    df_lineas.show(truncate=False)

    # --------------------------------------------------------------------------
    # CONSULTA SPARK SQL 2: Detección de Horas Punta y Nivel de Congestión
    # --------------------------------------------------------------------------
    print("\n" + "-"*80)
    print(" CONSULTA SPARK SQL 2: Deteccion de Horas Punta y Nivel de Congestion")
    print("-"*80)
    sql_franjas = """
        SELECT
            franja_horaria AS franja,
            MIN(hora) AS hora_inicio,
            MAX(hora) AS hora_fin,
            COUNT(*) AS total_desplazamientos,
            ROUND((COUNT(*) * 100.0) / 100000.0, 2) AS pct_sobre_total,
            FIRST(medio_transporte) AS medio_principal,
            CASE
                WHEN COUNT(*) >= 35000 THEN 'Critico (Punta)'
                WHEN COUNT(*) >= 20000 THEN 'Moderado'
                ELSE 'Fluido (Valle)'
            END AS nivel_congestion
        FROM movilidad_campus
        GROUP BY franja_horaria
        ORDER BY total_desplazamientos DESC
    """
    df_franjas = spark.sql(sql_franjas)
    df_franjas.show(truncate=False)

    # --------------------------------------------------------------------------
    # CONSULTA SPARK SQL 3: Orígenes Residenciales de Estudiantes (Conexión TPI 1)
    # --------------------------------------------------------------------------
    print("\n" + "-"*80)
    print(" CONSULTA SPARK SQL 3: Desplazamientos por Distrito y Ahorro de Emisiones")
    print("-"*80)
    sql_origenes = """
        SELECT
            distrito_origen,
            COUNT(*) AS total_viajeros,
            FIRST(medio_transporte) AS medio_mas_usado,
            ROUND(AVG(duracion_min), 2) AS tiempo_promedio_min,
            ROUND(AVG(distancia_km), 2) AS distancia_media_km,
            ROUND(SUM(co2_evitado_kg), 2) AS co2_evitado_kg
        FROM movilidad_campus
        GROUP BY distrito_origen
        ORDER BY total_viajeros DESC
    """
    df_origenes = spark.sql(sql_origenes)
    df_origenes.show(truncate=False)

    # --------------------------------------------------------------------------
    # CONSULTA SPARK SQL 4: Funciones de Ventana (Window Functions) - Rankings
    # --------------------------------------------------------------------------
    print("\n" + "-"*80)
    print(" CONSULTA SPARK SQL 4: Ranking Analitico de Medios mas Rapidos por Distrito")
    print("-"*80)
    sql_rankings = """
        WITH metricas_distrito AS (
            SELECT
                distrito_origen,
                medio_transporte,
                ROUND(AVG(duracion_min), 1) AS media_minutos,
                ROUND(AVG(satisfaccion_usuario), 2) AS nota_satisfaccion,
                COUNT(*) AS viajes
            FROM movilidad_campus
            GROUP BY distrito_origen, medio_transporte
        )
        SELECT
            distrito_origen,
            medio_transporte,
            media_minutos,
            nota_satisfaccion,
            viajes,
            DENSE_RANK() OVER (PARTITION BY distrito_origen ORDER BY media_minutos ASC) AS ranking_rapidez
        FROM metricas_distrito
        ORDER BY distrito_origen, ranking_rapidez
    """
    df_rankings = spark.sql(sql_rankings)
    df_rankings.show(15, truncate=False)

    # --------------------------------------------------------------------------
    # 4. INTEGRACIÓN CON SISTEMAS DE INFORMACIÓN EMPRESARIAL (Apartado 5.5 - JDBC)
    # --------------------------------------------------------------------------
    print("\n" + "="*80)
    print(" [4/5] INTEGRACION CON SISTEMA EMPRESARIAL (POSTGRESQL VIA JDBC)")
    print("="*80)
    
    jdbc_host = "postgres-empresa"  # Nombre del servicio en la red docker
    jdbc_port = "5432"
    jdbc_db = "universidad_empresa_db"
    jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_db}"
    
    connection_properties = {
        "user": "loyola_admin",
        "password": "loyola_password",
        "driver": "org.postgresql.Driver"
    }

    print(f"      Conectando a base de datos empresarial: {jdbc_url}...")
    
    try:
        # Volcado de las 3 tablas de KPIs analíticos
        print("      -> Volcando KPI 1: kpis_lineas_transporte...")
        df_lineas.write.jdbc(
            url=jdbc_url,
            table="kpis_lineas_transporte",
            mode="overwrite",
            properties=connection_properties
        )

        print("      -> Volcando KPI 2: kpis_franjas_horarias...")
        df_franjas.write.jdbc(
            url=jdbc_url,
            table="kpis_franjas_horarias",
            mode="overwrite",
            properties=connection_properties
        )

        print("      -> Volcando KPI 3: kpis_origenes_estudiantes...")
        df_origenes.write.jdbc(
            url=jdbc_url,
            table="kpis_origenes_estudiantes",
            mode="overwrite",
            properties=connection_properties
        )

        tiempo_total = round(time.time() - tiempo_inicio, 3)

        # Registrar auditoría en PostgreSQL
        audit_data = [(total_registros, tiempo_total, "COMPLETADO_EXITOSO", "Apache Spark 3.5.1 (Catalyst Engine)")]
        df_audit = spark.createDataFrame(audit_data, ["registros_analizados", "tiempo_procesamiento_seg", "estado", "motor"])
        df_audit.write.jdbc(
            url=jdbc_url,
            table="registro_procesamiento_spark",
            mode="append",
            properties=connection_properties
        )

        print("\n[5/5] EXITO: Todos los KPIs han sido integrados en PostgreSQL via JDBC.")
        print(f"      Tiempo total de ejecucion Spark: {tiempo_total} segundos.")
        print("="*80 + "\n")

    except Exception as e:
        print(f"[!] Error durante la integracion JDBC: {e}")
        # Si la conexion directa a la red docker falla en ejecucion aislada, mostramos el aviso
        tiempo_total = round(time.time() - tiempo_inicio, 3)

    spark.stop()

if __name__ == "__main__":
    main()
