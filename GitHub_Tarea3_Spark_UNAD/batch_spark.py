#!/usr/bin/env python3
"""Tarea 3 UNAD Big Data - Analisis batch con Apache Spark 3.5.x.
Datos de sensores SIMULADOS, no mediciones de un sistema real.
"""
from pathlib import Path
from html import escape
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

BASE = Path(__file__).resolve().parent
INPUT = str(BASE / 'datos_sensores_200000.csv')
OUT = BASE / 'resultados_batch'
OUT.mkdir(exist_ok=True)

spark = (SparkSession.builder.appName('UNAD_Tarea3_Batch_Sensores')
         .config('spark.sql.shuffle.partitions', '4')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')

schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('sensor_id', IntegerType(), True),
    StructField('temperature', DoubleType(), True),
    StructField('humidity', DoubleType(), True)
])
raw = spark.read.option('header', True).schema(schema).csv(INPUT)
initial_count = raw.count()
unique = raw.dropDuplicates()
unique_count = unique.count()

# Reglas: fecha y sensor validos; temperatura 0-40 C; humedad 0-100 %.
prepared = (unique
    .withColumn('fecha_hora', F.to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss'))
    .filter(F.col('fecha_hora').isNotNull() & F.col('sensor_id').between(1, 10))
    .withColumn('temperatura_limpia', F.when(F.col('temperature').between(0, 40), F.col('temperature')))
    .withColumn('humedad_limpia', F.when(F.col('humidity').between(0, 100), F.col('humidity')))
    .withColumn('fecha', F.to_date('fecha_hora')))

clean = prepared.filter(F.col('temperatura_limpia').isNotNull() & F.col('humedad_limpia').isNotNull()).cache()
valid_count = clean.count()
invalid_temp = prepared.filter(F.col('temperatura_limpia').isNull()).count()
invalid_humidity = prepared.filter(F.col('humedad_limpia').isNull()).count()

by_sensor = (clean.groupBy('sensor_id')
    .agg(F.count('*').alias('lecturas'),
         F.round(F.avg('temperatura_limpia'), 2).alias('temperatura_promedio'),
         F.round(F.avg('humedad_limpia'), 2).alias('humedad_promedio'),
         F.round(F.min('temperatura_limpia'), 2).alias('temperatura_minima'),
         F.round(F.max('temperatura_limpia'), 2).alias('temperatura_maxima'))
    .orderBy('sensor_id'))
by_day = clean.groupBy('fecha').agg(F.count('*').alias('lecturas')).orderBy('fecha')

print('\n========== CONTROL DE CALIDAD ==========', flush=True)
print('Filas originales:', initial_count)
print('Filas duplicadas:', initial_count - unique_count)
print('Registros con temperatura ausente o fuera de rango:', invalid_temp)
print('Registros con humedad ausente o fuera de rango:', invalid_humidity)
print('Filas validas para el analisis:', valid_count)
print('\n========== ESTADISTICAS POR SENSOR ==========', flush=True)
by_sensor.show(10, truncate=False)
print('\n========== LECTURAS POR FECHA ==========', flush=True)
by_day.show(25, truncate=False)

# Los datos agregados son pequenos; exportarlos con Python evita bibliotecas adicionales.
sensor_rows = [r.asDict() for r in by_sensor.collect()]
day_rows = [r.asDict() for r in by_day.collect()]
import csv
for filename, records, columns in [
    ('resumen_por_sensor.csv', sensor_rows, ['sensor_id', 'lecturas', 'temperatura_promedio', 'humedad_promedio', 'temperatura_minima', 'temperatura_maxima']),
    ('resumen_por_fecha.csv', day_rows, ['fecha', 'lecturas'])
]:
    with (OUT / filename).open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

def bar_svg(filename, title, labels, values, ylabel, fill):
    """Genera graficas SVG sin requerir pandas ni matplotlib."""
    w, h = 1050, 550
    left, right, top, bottom = 90, 35, 76, 105
    chart_w = w - left - right
    chart_h = h - top - bottom
    upper = max(values) * 1.18 if values and max(values) else 1
    n = max(len(values), 1)
    slot = chart_w / n
    bar = min(55, slot * .72)
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}">',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="{w/2}" y="37" text-anchor="middle" font-family="Arial" font-size="25" font-weight="bold">{escape(title)}</text>',
             f'<text x="24" y="{top+chart_h/2}" transform="rotate(-90 24,{top+chart_h/2})" text-anchor="middle" font-family="Arial" font-size="15">{escape(ylabel)}</text>']
    for t in range(5):
        value = upper * t / 4
        y = top + chart_h - chart_h * t / 4
        lines.extend([f'<line x1="{left}" y1="{y:.1f}" x2="{w-right}" y2="{y:.1f}" stroke="#e2e8f0"/>',
                      f'<text x="{left-12}" y="{y+5:.1f}" text-anchor="end" font-family="Arial" font-size="12" fill="#334155">{value:.1f}</text>'])
    for i, (label, value) in enumerate(zip(labels, values)):
        x = left + slot * (i + .5)
        height = chart_h * float(value) / upper
        y = top + chart_h - height
        lines.extend([f'<rect x="{x-bar/2:.1f}" y="{y:.1f}" width="{bar:.1f}" height="{height:.1f}" fill="{fill}" rx="3"/>',
                      f'<text x="{x:.1f}" y="{y-9:.1f}" text-anchor="middle" font-family="Arial" font-size="12">{value:.1f}</text>',
                      f'<text x="{x:.1f}" y="{top+chart_h+20}" text-anchor="middle" font-family="Arial" font-size="12">{escape(str(label))}</text>'])
    lines.append('</svg>')
    (OUT / filename).write_text('\n'.join(lines), encoding='utf-8')

bar_svg('temperatura_por_sensor.svg', 'Temperatura promedio por sensor (datos simulados)',
        [f'S{r["sensor_id"]}' for r in sensor_rows],
        [r['temperatura_promedio'] for r in sensor_rows],
        'Temperatura (°C)', '#2563eb')
bar_svg('lecturas_por_fecha.svg', 'Lecturas validas por fecha (datos simulados)',
        [str(r['fecha'])[5:] for r in day_rows],
        [r['lecturas'] for r in day_rows],
        'Cantidad de lecturas', '#0891b2')

report = [
    'INFORME DE EJECUCION - TAREA 3 BIG DATA UNAD',
    'Fuente: datos sinteticos simulados con generar_datos.py; NO son datos reales.',
    f'Registros originales: {initial_count}',
    f'Duplicados: {initial_count - unique_count}',
    f'Temperatura ausente o fuera de rango: {invalid_temp}',
    f'Humedad ausente o fuera de rango: {invalid_humidity}',
    f'Registros validos para analisis: {valid_count}',
    'Reglas de limpieza: quitar duplicados; fechas y sensores validos;',
 'conservar temperatura de 0 a 40 C y humedad de 0 a 100 %; descartar filas incompletas.',
    'Las graficas y CSV de resumen se encuentran en esta misma carpeta.'
]
(OUT / 'resumen_ejecucion.txt').write_text('\n'.join(report) + '\n', encoding='utf-8')
print('\nArchivos generados en:', OUT)
for p in sorted(OUT.iterdir()):
    print(' -', p.name)
clean.unpersist()
spark.stop()
