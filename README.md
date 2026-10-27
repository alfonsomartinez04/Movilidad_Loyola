# 🚌 Movilidad y Transporte en el Campus Loyola Sevilla
### Arquitectura Big Data y Procesamiento Analítico con Spark SQL e Integración Empresarial (PostgreSQL)

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)

**Asignatura:** Ampliación de Bases de Datos  
**Grado:** Grado en Ingeniería del Software (Curso 2026/2027)  
**Institución:** Universidad Loyola Andalucía (Campus Sevilla - Dos Hermanas)  
**Profesor:** Dr. Anier Soria Lorente  
**Estudiante:** Alfonso Nicolás Martínez Márquez  

---

## 📖 Descripción del Proyecto

Este repositorio contiene la implementación del **Trabajo Práctico Individual 2 (TPI 2)**, enfocado en el **Tema 5: Big Data y bases de datos a gran escala**.

Dando continuidad al TPI 1 (donde modelamos espacialmente las zonas de residencia estudiantil en Entrenúcleos, Montequinto y Bellavista mediante PostGIS), este proyecto analiza los flujos masivos de movilidad y transporte diario hacia el Campus de la Universidad Loyola:
* **Volumen:** Muestra realista de **100.000 eventos de desplazamiento** en el cuatrimestre académico.
* **Procesamiento Analítico (Apartado 5.4):** Consultas estructuradas en memoria con **Spark SQL** y optimización del motor Catalyst (detección de horas punta, saturación por líneas y ahorro de $\text{CO}_2$).
* **Integración Empresarial (Apartado 5.5):** Conexión vía **JDBC** para volcar las tablas analíticas agregadas en **PostgreSQL**, permitiendo al sistema corporativo de la universidad consultar los datos sin sobrecargarse.
* **Cuadro de Mando:** Visualización web interactiva de KPIs (`dashboard_movilidad.html`).

---

## 🏛️ Arquitectura del Sistema

```text
 [ Dataset Masivo 100k ]          [ Apache Spark 3.5.1 ]             [ PostgreSQL 16 ]
(CSV 23.6 MB de viajes)    ──>   (DataFrames en RAM + Catalyst) ──>  (Tablas consolidadas) ──> [ Dashboard Web ]
                              Consultas Spark SQL            (JDBC)   kpis_lineas_transporte     Chart.js KPIs
                                                                      kpis_franjas_horarias
```

---

## 🚀 Puesta en Marcha Rápida (Docker)

### 1. Clonar el repositorio
```bash
git clone https://github.com/alfonsomartinez04/Movilidad_Loyola.git
cd Movilidad_Loyola
```

### 2. Levantar los contenedores (Spark + PostgreSQL)
```bash
docker compose up -d
```
Verifica que los servicios están activos:
```bash
docker ps
```
* **Spark Master Web UI:** `http://localhost:8080`
* **PostgreSQL Empresarial:** Puerto local `5433` (Base de datos: `universidad_empresa_db`)

### 3. Ejecutar el Pipeline de Spark SQL
Lanza el procesamiento distribuido de las 100.000 filas y el volcado por JDBC:
```bash
docker exec -it loyola-spark-master /opt/spark/bin/spark-submit \
  --driver-class-path /opt/spark/work-dir/postgresql-42.7.3.jar \
  --jars /opt/spark/work-dir/postgresql-42.7.3.jar \
  /opt/spark/work-dir/procesar_spark_loyola.py
```
*Tiempo de procesamiento aproximado: **~13 segundos**.*

### 4. Consultar los resultados en PostgreSQL
```bash
docker exec -it loyola-postgres-empresa psql -U loyola_admin -d universidad_empresa_db -c "SELECT * FROM kpis_lineas_transporte;"
```

### 5. Ver el Cuadro de Mando Interactivo
Abre el archivo `dashboard_movilidad.html` en cualquier navegador web moderno (Google Chrome, Edge o Firefox).

---

## 📊 Consultas Spark SQL Implementadas

1. **Rendimiento por Línea:** Total de viajeros, duración media, retrasos y $\text{CO}_2$ emitido por cada medio (Metro L1 + Lanzadera, Autobús M-133, Bici, etc.).
2. **Detección de Horas Punta:** Clasificación de franjas horarias con `CASE WHEN` para detectar la hora punta matutina (07:30--09:30 h con el 44,88% de desplazamientos).
3. **Huella Ecológica por Distrito:** Comparativa del $\text{CO}_2$ evitado por estudiantes de Montequinto, Entrenúcleos y Bellavista al optar por transporte sostenible.
4. **Window Functions:** Ranking analítico de medios de transporte más veloces para cada barrio universitario mediante `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)`.

---

## 📄 Memoria del Trabajo

La memoria oficial en PDF con la justificación teórica, el desarrollo técnico, las capturas de ejecución y las referencias se encuentra disponible en la raíz del repositorio:
* [`TPI_2_Alfonso_Movilidad_BigData_Spark.pdf`](./TPI_2_Alfonso_Movilidad_BigData_Spark.pdf)
