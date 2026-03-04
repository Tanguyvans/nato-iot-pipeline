#!/usr/bin/env python3
"""
NATO IoT Pipeline - MQTT to InfluxDB with payload decoding
"""

import json
import base64
import struct
from datetime import datetime
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from datetime import datetime, timezone

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPICS = ["application/#"]

INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "sensors"

# InfluxDB client
influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)


def decode_sensecap_payload(data_bytes):
    """Decode SenseCAP S2120 weather station payload"""
    values = {}
    
    try:
        # SenseCAP S2120 format: starts with 0x4A
        if len(data_bytes) < 20:
            return values
            
        # Byte 0: 0x4A (header)
        # Bytes 1-2: Temperature (little-endian, 0.01°C)
        temp_raw = struct.unpack('<H', data_bytes[1:3])[0]
        values['temperature'] = temp_raw / 100.0
        
        # Bytes 3-4: Humidity (little-endian, 0.01%)
        hum_raw = struct.unpack('<H', data_bytes[3:5])[0]
        values['humidity'] = hum_raw / 100.0
        
        # Bytes 5-6: Light (lux)
        light_raw = struct.unpack('<H', data_bytes[5:7])[0]
        values['light'] = light_raw
        
        # Bytes 7-8: UV index (0.01)
        uv_raw = struct.unpack('<H', data_bytes[7:9])[0]
        values['uv_index'] = uv_raw / 100.0
        
        # Bytes 9-10: Wind speed (0.01 m/s)
        wind_raw = struct.unpack('<H', data_bytes[9:11])[0]
        values['wind_speed'] = wind_raw / 100.0
        
        # Byte 11: 0x4B (separator)
        
        # Bytes 12-13: Wind direction (degrees)
        wind_dir = struct.unpack('<H', data_bytes[12:14])[0]
        values['wind_direction'] = wind_dir
        
        # Bytes 14-17: Rainfall (0.001 mm)
        rain_raw = struct.unpack('<I', data_bytes[14:18])[0]
        values['rainfall'] = rain_raw / 1000.0
        
        # Bytes 18-21: Barometric pressure (0.01 hPa)
        pressure_raw = struct.unpack('<I', data_bytes[18:22])[0]
        values['pressure'] = pressure_raw / 100.0
        
    except Exception as e:
        print(f"SenseCAP decode error: {e}")
        values['raw_hex'] = data_bytes.hex()
    
    return values

def decode_milesight_payload(data_bytes):
    """Decode Milesight EM310-UDL ultrasonic sensor payload"""
    # Milesight uses channel-based format
    values = {}
    
    i = 0
    while i < len(data_bytes) - 2:
        channel = data_bytes[i]
        data_type = data_bytes[i + 1]
        
        if channel == 0x01 and data_type == 0x75:  # Battery (%)
            values['battery'] = data_bytes[i + 2]
            i += 3
        elif channel == 0x03 and data_type == 0x82:  # Distance (mm)
            values['distance'] = struct.unpack('<H', data_bytes[i+2:i+4])[0]
            i += 4
        elif channel == 0x04 and data_type == 0x00:  # Distance status
            values['distance_status'] = data_bytes[i + 2]
            i += 3
        else:
            i += 1
    
    return values


def decode_payload(device_name, application_name, data_base64):
    """Route to appropriate decoder based on device/application"""
    try:
        data_bytes = base64.b64decode(data_base64)
        
        if "meteo" in application_name.lower() or "sensecap" in device_name.lower():
            return decode_sensecap_payload(data_bytes)
        elif "ultrasonic" in application_name.lower() or "milesight" in device_name.lower():
            return decode_milesight_payload(data_bytes)
        else:
            return {"raw_hex": data_bytes.hex()}
    except Exception as e:
        print(f"Decode error: {e}")
        return {}


def on_connect(client, userdata, flags, reason_code, properties=None):
    """MQTT connect callback"""
    print(f"Connected to MQTT broker (rc={reason_code})")
    for topic in MQTT_TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")


def on_message(client, userdata, msg):
    """MQTT message callback"""
    try:
        payload = json.loads(msg.payload.decode())
        
        # Extract metadata
        device_name = payload.get("deviceName", "unknown")
        application_name = payload.get("applicationName", "unknown")
        dev_eui = payload.get("devEUI", "unknown")
        rssi = payload.get("rxInfo", [{}])[0].get("rssi", 0)
        snr = payload.get("rxInfo", [{}])[0].get("loRaSNR", 0)
        data_base64 = payload.get("data", "")
        
        # Decode sensor data
        sensor_values = decode_payload(device_name, application_name, data_base64)
        
        if sensor_values:
            # Build InfluxDB point
            point = {
                "measurement": application_name.lower().replace(" ", "_"),
                "tags": {
                    "device": device_name,
                    "dev_eui": dev_eui,
                },
                "fields": {
                    "rssi": rssi,
                    "snr": snr,
                    **sensor_values
                },
                "time": datetime.now(timezone.utc).isoformat()
            }
            
            # Write to InfluxDB
            influx_client.write_points([point])
            print(f"[{device_name}] {sensor_values}")
        
    except Exception as e:
        print(f"Error processing message: {e}")


def main():
    print("NATO IoT Pipeline starting...")
    
    # MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()