# NATO IoT Pipeline

Pipeline de collecte et visualisation des données IoT pour le projet NATO Smart City.

## Architecture

```
Capteurs LoRaWAN/Zigbee → WisGate (.238) → MQTT Broker (.231) → Python Pipeline → InfluxDB → Grafana
```

## Composants

| Composant | IP | Rôle |
|-----------|-----|------|
| WisGate RAK7268CV2 | 192.168.88.238 | Gateway LoRaWAN |
| iot-hub (RPi5) | 192.168.88.231 | MQTT Broker + Pipeline + InfluxDB + Grafana |
| rpi-nato (RPi5) | 192.168.88.247 | Zigbee2MQTT |

## Capteurs supportés

### LoRaWAN

- **SenseCAP S2120** : Station météo 8-en-1 (température, humidité, pression, vent, pluie, UV, lumière)
- **Milesight EM310-UDL** : Capteur ultrason (distance, niveau)

### Zigbee

- **Aqara Vibration Sensor** : Détection de vibrations

## Installation

### Prérequis

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients influxdb grafana python3-pip
pip3 install -r requirements.txt --break-system-packages
```

### Configuration InfluxDB

```bash
influx -execute "CREATE DATABASE sensors"
```

### Configuration Grafana

1. Accéder à <http://192.168.88.231:3000> (admin/admin)
2. Ajouter Data Source → InfluxDB
   - URL: <http://localhost:8086>
   - Database: sensors

### Démarrer le pipeline

#### Mode manuel

```bash
python3 mqtt_pipeline.py
```

#### Mode service (recommandé)

```bash
sudo cp nato-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nato-pipeline
sudo systemctl start nato-pipeline
```

#### Vérifier le status

```bash
sudo systemctl status nato-pipeline
sudo journalctl -u nato-pipeline -f
```

## Structure des données

### InfluxDB Measurements

**nato_meteo** (SenseCAP S2120)

- temperature (°C)
- humidity (%)
- light (lux)
- uv_index
- wind_speed (m/s)
- wind_direction (°)
- rainfall (mm/h)
- pressure (Pa)
- peak_wind_gust (m/s)
- rain_accumulation (mm)

**nato_ultrasonic** (Milesight EM310-UDL)

- battery (%)
- distance (mm)
- distance_status

## Topics MQTT

```
application/NATO_Meteo/device/+/rx      # SenseCAP S2120
application/NATO_ultrasonic/device/+/rx # Milesight EM310-UDL
```

## Décodage des payloads

Le script `mqtt_pipeline.py` décode automatiquement les payloads LoRaWAN :

- **SenseCAP firmware 2.0** : Format propriétaire avec blocs 4A/4B/4C
- **Milesight** : Format channel-based standard

## URLs d'accès

| Service | URL |
|---------|-----|
| Grafana | <http://192.168.88.231:3000> |
| WisGate | <http://192.168.88.238> |
| Zigbee2MQTT | <http://192.168.88.247:8080> |

## Fichiers

```
nato-iot-pipeline/
├── mqtt_pipeline.py      # Script principal
├── requirements.txt      # Dépendances Python
├── nato-pipeline.service # Service systemd
└── README.md
```
