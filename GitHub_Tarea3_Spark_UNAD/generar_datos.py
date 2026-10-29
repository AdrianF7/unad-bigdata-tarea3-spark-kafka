#!/usr/bin/env python3
"""Genera un conjunto de datos SIMULADOS y reproducibles para la Tarea 3."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

rng = random.Random(20261029)
base = datetime(2026, 10, 15, 0, 0, 0)
output = Path(__file__).resolve().parent / 'datos_sensores_200000.csv'
with output.open('w', newline='', encoding='utf-8') as fh:
    writer = csv.writer(fh)
    writer.writerow(['timestamp', 'sensor_id', 'temperature', 'humidity'])
    for i in range(200000):
        sensor_id = i % 10 + 1
        timestamp = base + timedelta(minutes=i // 10)
        temp = round(rng.uniform(20, 30) + 0.16 * (sensor_id - 5), 2)
        humidity = round(rng.uniform(30, 70), 2)
        if i % 7511 == 0:
            temp = ''
        elif i % 5000 == 0:
            temp = 43.5  # valor atipico, intencional
        if i % 9713 == 0:
            humidity = ''
        elif i % 7000 == 0:
            humidity = 109.0  # valor atipico, intencional
        row = [timestamp.strftime('%Y-%m-%d %H:%M:%S'), sensor_id, temp, humidity]
        writer.writerow(row)
        if i != 0 and i % 10000 == 0:
            writer.writerow(row)  # duplicado, intencional
print(f'Creado: {output} ({output.stat().st_size / 1024 / 1024:.1f} MiB)')
