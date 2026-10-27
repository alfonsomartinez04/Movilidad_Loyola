#!/usr/bin/env python3
"""
Generador de Dataset Masivo de Movilidad Universitaria
Campus Universidad Loyola Andalucía (Dos Hermanas - Sevilla)
Genera 100.000 registros de desplazamientos realistas para análisis Big Data con Spark SQL.
"""

import csv
import random
import datetime

NUM_REGISTROS = 100000
OUTPUT_FILE = "datos_movilidad_loyola_100k.csv"

# Orígenes conectados con el TPI 1 y el área metropolitana de Sevilla
ORIGENES = [
    ("Entrenúcleos", 2.2, 0.30),
    ("Montequinto", 4.5, 0.25),
    ("Bellavista - Sevilla Sur", 6.8, 0.15),
    ("Dos Hermanas Centro", 5.2, 0.12),
    ("Sevilla Centro / Nervión", 11.5, 0.10),
    ("Aljarafe / Tomares", 18.0, 0.05),
    ("Utrera / Los Palacios", 19.5, 0.03)
]

DESTINOS = [
    "Campus Loyola - Aulario Principal",
    "Campus Loyola - Biblioteca Central",
    "Campus Loyola - Polideportivo y Pistas",
    "Campus Loyola - Edificio Rectorado e Investigación"
]

# Modos de transporte y sus factores de emisión (kg CO2 por viajero y km)
# vs coche particular de combustión (~0.160 kg CO2 / km)
MODOS_TRANSPORTE = [
    {
        "medio": "Metro L1 + Bus Lanzadera",
        "linea": "Metro L1 (Olivar Quintos) + Lanzadera Loyola",
        "prob": 0.35,
        "velocidad_media": 28.0,  # km/h
        "co2_por_km": 0.025,
        "capacidad_media": 180
    },
    {
        "medio": "Autobus Metropolitano Consorcio",
        "linea": "Linea M-133 Sevilla (Prado) - Loyola",
        "prob": 0.22,
        "velocidad_media": 22.0,
        "co2_por_km": 0.040,
        "capacidad_media": 65
    },
    {
        "medio": "Bicicleta / Patinete Electrico",
        "linea": "Red Ciclista Metropolitana Entrenúcleos",
        "prob": 0.16,
        "velocidad_media": 16.0,
        "co2_por_km": 0.000,
        "capacidad_media": 1
    },
    {
        "medio": "Autobus Urbano Dos Hermanas",
        "linea": "Linea 5 Urbano DH (Plaza Arenal - Loyola)",
        "prob": 0.12,
        "velocidad_media": 19.0,
        "co2_por_km": 0.045,
        "capacidad_media": 60
    },
    {
        "medio": "Vehiculo Compartido (Carpooling)",
        "linea": "Carpooling App Estudiantes Loyola",
        "prob": 0.09,
        "velocidad_media": 45.0,
        "co2_por_km": 0.050,
        "capacidad_media": 4
    },
    {
        "medio": "Vehiculo Privado Individual",
        "linea": "Acceso Vial Autovía A-4 / SE-40",
        "prob": 0.06,
        "velocidad_media": 48.0,
        "co2_por_km": 0.160,
        "capacidad_media": 1
    }
]

def elegir_origen():
    r = random.random()
    acum = 0.0
    for nombre, dist, peso in ORIGENES:
        acum += peso
        if r <= acum:
            # Añadir variación de distancia +- 15%
            dist_var = dist * random.uniform(0.85, 1.15)
            return nombre, round(dist_var, 2)
    return ORIGENES[0][0], ORIGENES[0][1]

def elegir_modo():
    r = random.random()
    acum = 0.0
    for modo in MODOS_TRANSPORTE:
        acum += modo["prob"]
        if r <= acum:
            return modo
    return MODOS_TRANSPORTE[0]

def generar_dataset():
    print(f"Generando {NUM_REGISTROS} registros masivos de movilidad...")
    
    # Rango de fechas: Primer cuatrimestre 2026/2027 (15 sep 2026 a 18 dic 2026)
    fecha_inicio = datetime.date(2026, 9, 15)
    dias_totales = 95
    
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id_viaje",
            "timestamp",
            "fecha",
            "hora",
            "dia_semana",
            "franja_horaria",
            "distrito_origen",
            "destino",
            "medio_transporte",
            "linea_servicio",
            "distancia_km",
            "duracion_min",
            "retraso_min",
            "ocupacion_unidad_pct",
            "co2_emitido_kg",
            "co2_evitado_kg",
            "satisfaccion_usuario"
        ])
        
        id_viaje = 100001
        for i in range(NUM_REGISTROS):
            # Selección de fecha (solo lunes a viernes)
            while True:
                delta = random.randint(0, dias_totales)
                fecha = fecha_inicio + datetime.timedelta(days=delta)
                if fecha.weekday() < 5:  # 0=Lunes, 4=Viernes
                    break
            
            dias_str = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes"]
            dia_semana = dias_str[fecha.weekday()]
            
            # Distribución realista de horas (horas punta matutina y de mediodía)
            prob_hora = random.random()
            if prob_hora < 0.45:
                # Hora punta mañana (7:30 - 9:30)
                hora = random.choices([7, 8, 9], weights=[15, 60, 25])[0]
                minuto = random.randint(0, 59)
                franja = "Punta Mañana (07:30-09:30)"
                factor_congestion = random.uniform(1.20, 1.50)
            elif prob_hora < 0.75:
                # Hora punta mediodía / salida (13:30 - 15:30)
                hora = random.choices([13, 14, 15], weights=[20, 55, 25])[0]
                minuto = random.randint(0, 59)
                franja = "Punta Mediodía (13:30-15:30)"
                factor_congestion = random.uniform(1.15, 1.40)
            elif prob_hora < 0.90:
                # Mañana valle (10:00 - 13:00)
                hora = random.randint(10, 12)
                minuto = random.randint(0, 59)
                franja = "Valle Mañana (10:00-13:00)"
                factor_congestion = 1.00
            else:
                # Tarde / Noche (16:00 - 21:00)
                hora = random.randint(16, 21)
                minuto = random.randint(0, 59)
                franja = "Tarde/Noche (16:00-21:00)"
                factor_congestion = random.uniform(1.05, 1.20)
            
            segundo = random.randint(0, 59)
            dt = datetime.datetime(fecha.year, fecha.month, fecha.day, hora, minuto, segundo)
            
            origen, dist_km = elegir_origen()
            destino = random.choice(DESTINOS)
            modo = elegir_modo()
            
            # Cálculo de duración base y retraso
            duracion_base = (dist_km / modo["velocidad_media"]) * 60.0
            duracion_real = duracion_base * factor_congestion * random.uniform(0.95, 1.10)
            
            # Retraso (mayor en horas punta y autobuses)
            if factor_congestion > 1.1 and "Autobus" in modo["medio"]:
                retraso = round(random.uniform(2.0, 14.0), 1)
            elif factor_congestion > 1.1 and "Metro" in modo["medio"]:
                retraso = round(random.uniform(0.5, 4.0), 1)
            else:
                retraso = round(random.uniform(0.0, 3.0), 1)
                
            duracion_final = round(duracion_real + retraso, 1)
            
            # Ocupación de la unidad de transporte (%)
            if "Punta" in franja:
                ocupacion = round(random.uniform(72.0, 98.0), 1)
            else:
                ocupacion = round(random.uniform(30.0, 65.0), 1)
                
            # Huella de carbono (kg CO2)
            co2_emitido = round(dist_km * modo["co2_por_km"], 3)
            # CO2 evitado respecto a ir en coche solo (0.160 kg/km)
            co2_coche_particular = dist_km * 0.160
            co2_evitado = round(max(0.0, co2_coche_particular - co2_emitido), 3)
            
            # Satisfacción del usuario (1 a 5 estrellas)
            if retraso > 8.0:
                satisfaccion = random.choice([1, 2, 3])
            elif retraso < 2.0 and modo["medio"] == "Bicicleta / Patinete Electrico":
                satisfaccion = random.choice([4, 5])
            else:
                satisfaccion = random.choice([3, 4, 5])
                
            writer.writerow([
                id_viaje,
                dt.strftime("%Y-%m-%d %H:%M:%S"),
                fecha.strftime("%Y-%m-%d"),
                hora,
                dia_semana,
                franja,
                origen,
                destino,
                modo["medio"],
                modo["linea"],
                dist_km,
                duracion_final,
                retraso,
                ocupacion,
                co2_emitido,
                co2_evitado,
                satisfaccion
            ])
            id_viaje += 1

    print(f"Dataset generado con exito en {OUTPUT_FILE} ({NUM_REGISTROS} filas)")

if __name__ == "__main__":
    generar_dataset()
