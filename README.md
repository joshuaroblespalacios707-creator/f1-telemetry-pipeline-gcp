**🇲🇽 Español** | [🇬🇧 English](#-f1-25-real-time-telemetry-pipeline-on-google-cloud-platform)

# 🏎️ Pipeline de Telemetría en Tiempo Real de F1 25 en Google Cloud Platform

*Construido para F1 25 — adaptable a cualquier simulador de carreras con salida de telemetría UDP.*

Una arquitectura de streaming de datos en tiempo real, de nivel empresarial, diseñada para capturar, procesar y analizar telemetría UDP de F1 25 usando Google Cloud Platform (GCP), incluyendo temperatura y presión de llantas, temperatura de frenos en las cuatro esquinas, y métricas de conducción fotograma a fotograma.

> 🚧 **Estado: Beta abierta.** Este proyecto nació para F1 25, pero el protocolo UDP de telemetría es similar en otros juegos de carreras (iRacing, Assetto Corsa, Forza, Gran Turismo, otras entregas de F1). Si corres algún otro título con salida de telemetría UDP, siéntete libre de adaptar el parser (`f1_publisher.py`) a tu juego y mandar un PR. La idea es que esto deje de ser "mi proyecto" y se vuelva una base que cualquier sim racer pueda usar y mejorar.

---

## 🤝 Cómo Contribuir / Adaptar a Otro Juego

1. **Identifica el formato UDP de tu juego** — la mayoría de los simuladores de carreras documentan su estructura de paquetes (busca "UDP telemetry format" + el nombre de tu juego).
2. **Ajusta el `struct.unpack`** en `f1_publisher.py` al layout de bytes de tu juego — cada título tiene su propio orden de campos.
3. **Cambia el esquema de BigQuery** (`telemetry_table`) si tu juego expone métricas distintas.
4. **Documenta tu adaptación** en un fork o PR, idealmente con el nombre del juego en el nombre del archivo (ej. `iracing_publisher.py`).

---

## 🏗️ Arquitectura del Sistema

El sistema captura, procesa e ingiere en tiempo real la telemetría enviada por el videojuego F1 25, mediante un pipeline serverless y de cómputo en GCP.

```
[ F1 25 Game (PC) ]
       │ UDP Port 20777
       ▼
[ Compute Engine VM ] ──── (Script f1_publisher.py ejecutado por systemd)
       │ Pub/Sub API
       ▼
[ Cloud Pub/Sub ]
       │ BigQuery Subscription
       ▼
[ BigQuery Dataset ] ─── (f1_dataset.telemetry_table)
```

| Componente | Recurso de GCP | Función |
|---|---|---|
| Servidor de ingesta | Compute Engine (`e2-micro`) | Escucha los paquetes UDP entrantes en el puerto 20777, desempaqueta los datos binarios, y envía payloads JSON a Pub/Sub |
| Message broker | Cloud Pub/Sub | Amortigua eventos de streaming de alto throughput con latencia sub-segundo |
| Data warehouse | BigQuery | Almacena los registros de telemetría en JSON crudo (`data:JSON`) para consultas analíticas de bajo costo y alta velocidad |
| Daemon de fondo | Servicio systemd | Mantiene el script de ingesta en Python corriendo 24/7 en la VM como proceso persistente |

---

## 🌐 Configuración de Red e Infraestructura

### Regla de Firewall en GCP

Permite el ingreso de tramas UDP emitidas desde el simulador de carreras hacia la interfaz pública de la VM:

| Parámetro | Valor |
|---|---|
| Nombre de regla | `allow-f1-udp` |
| Protocolo y puerto | `udp:20777` |
| Origen | `0.0.0.0/0` (ingreso) |

### Servicio de Sistema (systemd)

Ubicación de la unidad: `/etc/systemd/system/f1-telemetry.service`

```bash
sudo systemctl start f1-telemetry.service
sudo systemctl restart f1-telemetry.service
sudo journalctl -u f1-telemetry.service -f
```

---

## 📦 Protocolo UDP y Script Publicador

El script escucha en el puerto 20777 y decodifica el **Packet ID 6** (*Car Telemetry Packet*), desempaquetando los parámetros de velocidad, marchas, pedales, temperaturas de llantas, presiones en PSI, y temperaturas de frenos en las cuatro esquinas (RL, RR, FL, FR).

```python
import socket
import struct
import json
from google.cloud import pubsub_v1

PROJECT_ID = "<PROJECT_ID>"
TOPIC_ID = "f1-telemetry-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

UDP_IP = "0.0.0.0"
UDP_PORT = 20777

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

# Encabezado oficial F1 25 (29 bytes)
HEADER_FORMAT = '<HBBBBBQfIIBB'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
CAR_TELEMETRY_SIZE = 60

while True:
    try:
        data, addr = sock.recvfrom(2048)
        if len(data) < HEADER_SIZE:
            continue

        header = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        packet_id = header[5]
        player_car_index = header[10]

        # Packet ID 6 = Car Telemetry Packet
        if packet_id == 6:
            car_offset = HEADER_SIZE + (player_car_index * CAR_TELEMETRY_SIZE)
            if len(data) < car_offset + CAR_TELEMETRY_SIZE:
                continue

            speed = struct.unpack_from('<H', data, car_offset + 0)[0]
            throttle = struct.unpack_from('<f', data, car_offset + 2)[0]
            steer = struct.unpack_from('<f', data, car_offset + 6)[0]
            brake = struct.unpack_from('<f', data, car_offset + 10)[0]
            gear = struct.unpack_from('<b', data, car_offset + 15)[0]
            engine_rpm = struct.unpack_from('<H', data, car_offset + 16)[0]

            # Desempaquetado de arreglos térmicos y de presión [RL, RR, FL, FR]
            brakes_temp = list(struct.unpack_from('<HHHH', data, car_offset + 22))
            tyres_surface_temp = list(struct.unpack_from('<BBBB', data, car_offset + 30))
            tyres_inner_temp = list(struct.unpack_from('<BBBB', data, car_offset + 34))
            engine_temp = struct.unpack_from('<H', data, car_offset + 38)[0]
            tyres_pressure = list(struct.unpack_from('<ffff', data, car_offset + 40))

            telemetry_payload = {
                "frame_id": header[8],
                "session_time": round(header[7], 2),
                "speed": speed,
                "throttle": round(throttle, 2),
                "steer": round(steer, 2),
                "brake": round(brake, 2),
                "gear": gear,
                "engine_rpm": engine_rpm,
                "engine_temp": engine_temp,
                "tyres_surface_temp": tyres_surface_temp,
                "tyres_inner_temp": tyres_inner_temp,
                "tyres_pressure": [round(p, 2) for p in tyres_pressure],
                "brakes_temp": brakes_temp
            }

            publisher.publish(topic_path, json.dumps(telemetry_payload).encode("utf-8"))
            print(f"🔥 [Frame {header[8]}] Vel: {speed} km/h | Llantas °C: {tyres_surface_temp} | Frenos °C: {brakes_temp}", flush=True)
    except Exception:
        pass
```

---

## 📊 Consultas Analíticas en BigQuery Studio

### Consulta A — Telemetría Fotograma a Fotograma (Últimos Registros)

```sql
SELECT
    INT64(data.frame_id) AS frame_id,
    ROUND(FLOAT64(data.session_time), 2) AS session_time,
    INT64(data.speed) AS velocidad_kmh,
    INT64(data.gear) AS marcha,
    INT64(data.engine_rpm) AS rpm,

    -- Temperatura de llantas (°C) [RL, RR, FL, FR]
    INT64(data.tyres_surface_temp[0]) AS temp_tras_izq_c,
    INT64(data.tyres_surface_temp[1]) AS temp_tras_der_c,
    INT64(data.tyres_surface_temp[2]) AS temp_del_izq_c,
    INT64(data.tyres_surface_temp[3]) AS temp_del_der_c,

    -- Temperatura de frenos (°C) [RL, RR, FL, FR]
    INT64(data.brakes_temp[0]) AS freno_tras_izq_c,
    INT64(data.brakes_temp[1]) AS freno_tras_der_c,
    INT64(data.brakes_temp[2]) AS freno_del_izq_c,
    INT64(data.brakes_temp[3]) AS freno_del_der_c,

    publish_time
FROM
    `<PROJECT_ID>.f1_dataset.telemetry_table`
ORDER BY
    publish_time DESC
LIMIT 50;
```

### Consulta B — Resumen Ejecutivo de Sesión Actual

```sql
SELECT
    COUNT(1) AS marcos_sesion_actual,
    ROUND((MAX(FLOAT64(data.session_time)) - MIN(FLOAT64(data.session_time))) / 60, 2) AS minutos_en_pista,
    MAX(INT64(data.speed)) AS velocidad_maxima_kmh,
    ROUND(AVG(INT64(data.speed)), 1) AS velocidad_promedio_kmh,

    -- Promedio térmico de llantas (°C)
    ROUND(AVG(INT64(data.tyres_surface_temp[0])), 1) AS temp_prom_tras_izq,
    ROUND(AVG(INT64(data.tyres_surface_temp[1])), 1) AS temp_prom_tras_der,
    ROUND(AVG(INT64(data.tyres_surface_temp[2])), 1) AS temp_prom_del_izq,
    ROUND(AVG(INT64(data.tyres_surface_temp[3])), 1) AS temp_prom_del_der,

    -- Promedio térmico de frenos delanteros (°C)
    ROUND(AVG(INT64(data.brakes_temp[2])), 1) AS freno_prom_del_izq,
    ROUND(AVG(INT64(data.brakes_temp[3])), 1) AS freno_prom_del_der
FROM
    `<PROJECT_ID>.f1_dataset.telemetry_table`
WHERE
    publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    AND INT64(data.speed) BETWEEN 1 AND 360;
```

---

## 🔧 Bitácora de Resolución de Problemas

### Discriminación de tipo de paquete UDP
- **Causa raíz:** la versión inicial procesaba el Packet ID 0 (*Motion*), ignorando el identificador Packet ID 6 (*Car Telemetry*), que es el que realmente contiene los datos de llantas y frenos.
- **Solución:** mapeo explícito del desplazamiento binario del encabezado, para validar el tipo de paquete antes de desempaquetar.

### Integridad del código en la VM
- **Causa raíz:** inserciones incompletas vía SSH truncaban la sintaxis del bucle principal del script.
- **Solución:** edición y verificación directa usando `nano /home/$USER/f1_publisher.py`.

### Manejo de datos históricos en BigQuery
- **Causa raíz:** la existencia de más de 60,000 registros históricos sin claves de llantas descartaba filas al usar cláusulas `WHERE` estrictas.
- **Solución:** filtrado por ventana temporal (`TIMESTAMP_SUB`) para aislar únicamente las sesiones activas.

---

## 🎓 Conceptos Aplicados

- Ingesta de datos en tiempo real desde un protocolo UDP binario hacia una arquitectura cloud-native
- Desempaquetado de estructuras binarias (`struct.unpack`) para parsear paquetes de telemetría propietarios, incluyendo arreglos térmicos de 4 posiciones
- Discriminación de tipos de paquete (Motion vs. Car Telemetry) mediante offsets explícitos del encabezado binario
- Streaming desacoplado mediante Pub/Sub, con suscripción directa a BigQuery
- Uso de columnas de tipo `JSON` nativo en BigQuery para esquemas semi-estructurados con arreglos anidados
- Manejo de drift de esquema en datos históricos mediante filtrado por ventana temporal
- Persistencia de procesos en Linux mediante `systemd`, con reinicio automático ante fallos

---

## 📌 Notas

Este proyecto nació de una necesidad personal real: entender mi propio desempeño corriendo F1 25 (ritmo, gestión térmica de llantas y frenos, eficiencia de cambios) con datos cuantificables, en vez de solo sensaciones. Es un ejemplo end-to-end de arquitectura de streaming en la nube, aplicado a un caso de uso fuera del contexto típico de laboratorio.

---
---

[⬆ Español version above](#-pipeline-de-telemetría-en-tiempo-real-de-f1-25-en-google-cloud-platform)

# 🏎️ F1 25 Real-Time Telemetry Pipeline on Google Cloud Platform

*Built for F1 25 — adaptable to any racing sim with UDP telemetry output.*

An enterprise-grade, real-time data streaming architecture designed to capture, process, and analyze UDP telemetry from F1 25 using Google Cloud Platform (GCP), including tire temperature and pressure, four-corner brake temperature, and frame-by-frame driving metrics.

> 🚧 **Status: Open beta.** This project was built for F1 25, but the UDP telemetry protocol is similar across other racing titles (iRacing, Assetto Corsa, Forza, Gran Turismo, other F1 entries). If you play another sim with UDP telemetry output, feel free to adapt the parser (`f1_publisher.py`) to your game and send a PR. The goal is for this to stop being "my project" and become a base any sim racer can use and improve.

---

## 🤝 How to Contribute / Adapt to Another Game

1. **Identify your game's UDP format** — most racing sims document their packet structure (search "UDP telemetry format" + your game's name).
2. **Adjust the `struct.unpack`** in `f1_publisher.py` to match your game's byte layout — each title has its own field ordering.
3. **Update the BigQuery schema** (`telemetry_table`) if your game exposes different metrics.
4. **Document your adaptation** in a fork or PR, ideally naming the file after the game (e.g., `iracing_publisher.py`).

---

## 🏗️ System Architecture

The system captures, processes, and ingests in real time the telemetry sent by the F1 25 video game, via a serverless and compute pipeline on GCP.

```
[ F1 25 Game (PC) ]
       │ UDP Port 20777
       ▼
[ Compute Engine VM ] ──── (f1_publisher.py script run by systemd)
       │ Pub/Sub API
       ▼
[ Cloud Pub/Sub ]
       │ BigQuery Subscription
       ▼
[ BigQuery Dataset ] ─── (f1_dataset.telemetry_table)
```

| Component | GCP Resource | Function |
|---|---|---|
| Ingestion server | Compute Engine (`e2-micro`) | Listens to incoming UDP packets on port 20777, unpacks binary data, and streams JSON payloads to Pub/Sub |
| Message broker | Cloud Pub/Sub | Buffers high-throughput streaming events with sub-second latency |
| Data warehouse | BigQuery | Stores raw JSON telemetry records (`data:JSON`) for low-cost, high-speed analytical querying |
| Background daemon | systemd Service | Keeps the Python ingestion script running 24/7 on the VM as a persistent process |

---

## 🌐 Network & Infrastructure Configuration

### GCP Firewall Rule

Allows inbound UDP frames sent from the racing sim toward the VM's public interface:

| Parameter | Value |
|---|---|
| Rule name | `allow-f1-udp` |
| Protocol & port | `udp:20777` |
| Source | `0.0.0.0/0` (ingress) |

### System Service (systemd)

Unit location: `/etc/systemd/system/f1-telemetry.service`

```bash
sudo systemctl start f1-telemetry.service
sudo systemctl restart f1-telemetry.service
sudo journalctl -u f1-telemetry.service -f
```

---

## 📦 UDP Protocol & Publisher Script

The script listens on port 20777 and decodes **Packet ID 6** (*Car Telemetry Packet*), unpacking speed, gear, pedal, tire temperature, PSI pressure, and four-corner brake temperature (RL, RR, FL, FR) parameters.

```python
import socket
import struct
import json
from google.cloud import pubsub_v1

PROJECT_ID = "<PROJECT_ID>"
TOPIC_ID = "f1-telemetry-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

UDP_IP = "0.0.0.0"
UDP_PORT = 20777

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

# Official F1 25 header (29 bytes)
HEADER_FORMAT = '<HBBBBBQfIIBB'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
CAR_TELEMETRY_SIZE = 60

while True:
    try:
        data, addr = sock.recvfrom(2048)
        if len(data) < HEADER_SIZE:
            continue

        header = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        packet_id = header[5]
        player_car_index = header[10]

        # Packet ID 6 = Car Telemetry Packet
        if packet_id == 6:
            car_offset = HEADER_SIZE + (player_car_index * CAR_TELEMETRY_SIZE)
            if len(data) < car_offset + CAR_TELEMETRY_SIZE:
                continue

            speed = struct.unpack_from('<H', data, car_offset + 0)[0]
            throttle = struct.unpack_from('<f', data, car_offset + 2)[0]
            steer = struct.unpack_from('<f', data, car_offset + 6)[0]
            brake = struct.unpack_from('<f', data, car_offset + 10)[0]
            gear = struct.unpack_from('<b', data, car_offset + 15)[0]
            engine_rpm = struct.unpack_from('<H', data, car_offset + 16)[0]

            # Unpacking thermal and pressure arrays [RL, RR, FL, FR]
            brakes_temp = list(struct.unpack_from('<HHHH', data, car_offset + 22))
            tyres_surface_temp = list(struct.unpack_from('<BBBB', data, car_offset + 30))
            tyres_inner_temp = list(struct.unpack_from('<BBBB', data, car_offset + 34))
            engine_temp = struct.unpack_from('<H', data, car_offset + 38)[0]
            tyres_pressure = list(struct.unpack_from('<ffff', data, car_offset + 40))

            telemetry_payload = {
                "frame_id": header[8],
                "session_time": round(header[7], 2),
                "speed": speed,
                "throttle": round(throttle, 2),
                "steer": round(steer, 2),
                "brake": round(brake, 2),
                "gear": gear,
                "engine_rpm": engine_rpm,
                "engine_temp": engine_temp,
                "tyres_surface_temp": tyres_surface_temp,
                "tyres_inner_temp": tyres_inner_temp,
                "tyres_pressure": [round(p, 2) for p in tyres_pressure],
                "brakes_temp": brakes_temp
            }

            publisher.publish(topic_path, json.dumps(telemetry_payload).encode("utf-8"))
            print(f"🔥 [Frame {header[8]}] Vel: {speed} km/h | Llantas °C: {tyres_surface_temp} | Frenos °C: {brakes_temp}", flush=True)
    except Exception:
        pass
```

---

## 📊 Analytics Queries in BigQuery Studio

### Query A — Frame-by-Frame Telemetry (Latest Records)

```sql
SELECT
    INT64(data.frame_id) AS frame_id,
    ROUND(FLOAT64(data.session_time), 2) AS session_time,
    INT64(data.speed) AS speed_kmh,
    INT64(data.gear) AS gear,
    INT64(data.engine_rpm) AS rpm,

    -- Tire temperature (°C) [RL, RR, FL, FR]
    INT64(data.tyres_surface_temp[0]) AS temp_rl_c,
    INT64(data.tyres_surface_temp[1]) AS temp_rr_c,
    INT64(data.tyres_surface_temp[2]) AS temp_fl_c,
    INT64(data.tyres_surface_temp[3]) AS temp_fr_c,

    -- Brake temperature (°C) [RL, RR, FL, FR]
    INT64(data.brakes_temp[0]) AS brake_rl_c,
    INT64(data.brakes_temp[1]) AS brake_rr_c,
    INT64(data.brakes_temp[2]) AS brake_fl_c,
    INT64(data.brakes_temp[3]) AS brake_fr_c,

    publish_time
FROM
    `<PROJECT_ID>.f1_dataset.telemetry_table`
ORDER BY
    publish_time DESC
LIMIT 50;
```

### Query B — Current Session Executive Summary

```sql
SELECT
    COUNT(1) AS current_session_frames,
    ROUND((MAX(FLOAT64(data.session_time)) - MIN(FLOAT64(data.session_time))) / 60, 2) AS track_time_minutes,
    MAX(INT64(data.speed)) AS top_speed_kmh,
    ROUND(AVG(INT64(data.speed)), 1) AS avg_speed_kmh,

    -- Average tire temperature (°C)
    ROUND(AVG(INT64(data.tyres_surface_temp[0])), 1) AS avg_temp_rl,
    ROUND(AVG(INT64(data.tyres_surface_temp[1])), 1) AS avg_temp_rr,
    ROUND(AVG(INT64(data.tyres_surface_temp[2])), 1) AS avg_temp_fl,
    ROUND(AVG(INT64(data.tyres_surface_temp[3])), 1) AS avg_temp_fr,

    -- Average front brake temperature (°C)
    ROUND(AVG(INT64(data.brakes_temp[2])), 1) AS avg_brake_fl,
    ROUND(AVG(INT64(data.brakes_temp[3])), 1) AS avg_brake_fr
FROM
    `<PROJECT_ID>.f1_dataset.telemetry_table`
WHERE
    publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    AND INT64(data.speed) BETWEEN 1 AND 360;
```

---

## 🔧 Troubleshooting Log

### UDP packet type discrimination
- **Root cause:** the initial version processed Packet ID 0 (*Motion*), ignoring Packet ID 6 (*Car Telemetry*), which is the one that actually contains tire and brake data.
- **Resolution:** explicit mapping of the header's binary offset, to validate the packet type before unpacking.

### Code integrity on the VM
- **Root cause:** incomplete pastes via SSH truncated the main loop's syntax.
- **Resolution:** direct editing and verification using `nano /home/$USER/f1_publisher.py`.

### Handling historical data in BigQuery
- **Root cause:** over 60,000 historical records with no tire-related keys caused rows to be dropped when using strict `WHERE` clauses.
- **Resolution:** filtering by time window (`TIMESTAMP_SUB`) to isolate only active sessions.

---

## 🎓 Concepts Applied

- Real-time ingestion from a binary UDP protocol into a cloud-native architecture
- Binary structure unpacking (`struct.unpack`) to parse proprietary telemetry packets, including 4-position thermal arrays
- Packet type discrimination (Motion vs. Car Telemetry) via explicit binary header offsets
- Decoupled streaming via Pub/Sub, with a direct BigQuery subscription
- Using native `JSON` columns in BigQuery for semi-structured schemas with nested arrays
- Handling schema drift in historical data via time-window filtering
- Process persistence on Linux via `systemd`, with automatic restart on failure

---

## 📌 Notes

This project came from a real personal need: understanding my own performance racing F1 25 (pace, tire and brake thermal management, shift efficiency) with quantifiable data, rather than just gut feeling. It's an end-to-end example of a cloud streaming architecture, applied to a use case outside the typical lab context.
