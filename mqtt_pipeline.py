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
        # SenseCAP S2120 uses measurement ID format
        # Format: [MeasurementID (2 bytes)] [Value (variable)]
        
        i = 0
        while i < len(data_bytes) - 2:
            # Measurement ID is 2 bytes
            if i + 2 > len(data_bytes):
                break
                
            meas_id = struct.unpack('<H', data_bytes[i:i+2])[0]
            i += 2
            
            # 0x004A (74) = Air Temperature (°C * 10)
            if meas_id == 0x004A:
                if i + 2 <= len(data_bytes):
                    val = struct.unpack('<h', data_bytes[i:i+2])[0]
                    values['temperature'] = val / 10.0
                    i += 2
                    
            # 0x29D7 = might be pressure
            elif meas_id == 0x29D7:
                i += 4  # skip for now
                
            # 0x014B (331) = Wind Direction (degrees)
            elif meas_id == 0x014B:
                if i + 2 <= len(data_bytes):
                    val = struct.unpack('<H', data_bytes[i:i+2])[0]
                    values['wind_direction'] = val
                    i += 2
                    
            # 0x4C27 or 0x274C = Barometric Pressure
            elif meas_id == 0xCF27:
                if i + 4 <= len(data_bytes):
                    val = struct.unpack('<I', data_bytes[i:i+4])[0]
                    values['pressure'] = val / 100.0
                    i += 4
                    
            # 0xDC11 = might be rainfall
            elif meas_id == 0xDC11:
                if i + 2 <= len(data_bytes):
                    val = struct.unpack('<H', data_bytes[i:i+2])[0]
                    values['rainfall'] = val / 10.0
                    i += 2
            else:
                i += 1  # skip unknown byte
                
        # If no values decoded, store raw hex
        if not values:
            values['raw_hex'] = data_bytes.hex()
            
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