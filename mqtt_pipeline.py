#!/usr/bin/env python3
"""
NATO IoT Pipeline - MQTT to InfluxDB with payload decoding
"""

import json
import base64
import struct
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPICS = ["application/#", "zigbee2mqtt/#"]

INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "sensors"

# InfluxDB client
influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)


def decode_sensecap_payload(data_bytes):
    """Decode SenseCAP S2120 firmware 2.0 (fPort 3)"""
    values = {}
    
    try:
        # Convert to hex string
        data = data_bytes.hex().upper()
        
        i = 0
        while i < len(data) - 2:
            data_id = data[i:i+2]
            
            if data_id == '4A':
                data_value = data[i+2:i+22]
                if len(data_value) >= 20:
                    values['temperature'] = loraWANV2DataFormat(data_value[0:4], 10)
                    values['humidity'] = loraWANV2DataFormat(data_value[4:6], 1)
                    values['light'] = loraWANV2DataFormat(data_value[6:14], 1)
                    values['uv_index'] = loraWANV2DataFormat(data_value[14:16], 10)
                    values['wind_speed'] = loraWANV2DataFormat(data_value[16:20], 10)
                i += 22
                
            elif data_id == '4B':
                data_value = data[i+2:i+18]
                if len(data_value) >= 16:
                    values['wind_direction'] = loraWANV2DataFormat(data_value[0:4], 1)
                    values['rainfall'] = loraWANV2DataFormat(data_value[4:12], 1000)
                    values['pressure'] = loraWANV2DataFormat(data_value[12:16], 0.1)
                i += 18
                
            elif data_id == '4C':
                data_value = data[i+2:i+14]
                if len(data_value) >= 12:
                    values['peak_wind_gust'] = loraWANV2DataFormat(data_value[0:4], 10)
                    values['rain_accumulation'] = loraWANV2DataFormat(data_value[4:12], 1000)
                i += 14
                
            else:
                i += 2
                
    except Exception as e:
        print(f"SenseCAP decode error: {e}")
        values['raw_hex'] = data_bytes.hex()
    
    return values


def loraWANV2DataFormat(hex_str, divisor=1):
    """Convert big-endian hex string to value with divisor"""
    try:
        bytes_arr = [hex_str[i:i+2] for i in range(0, len(hex_str), 2)]
        binary_str = ''.join(format(int(b, 16), '08b') for b in bytes_arr)
        
        if binary_str[0] == '1':
            inverted = ''.join('0' if b == '1' else '1' for b in binary_str)
            value = -(int(inverted, 2) + 1)
        else:
            value = int(binary_str, 2)
        
        return value / divisor
    except:
        return 0


def decode_milesight_payload(data_bytes):
    """Decode Milesight EM310-UDL ultrasonic sensor payload"""
    values = {}
    
    i = 0
    while i < len(data_bytes) - 2:
        channel = data_bytes[i]
        data_type = data_bytes[i + 1]
        
        if channel == 0x01 and data_type == 0x75:
            values['battery'] = data_bytes[i + 2]
            i += 3
        elif channel == 0x03 and data_type == 0x82:
            values['distance'] = struct.unpack('<H', data_bytes[i+2:i+4])[0]
            i += 4
        elif channel == 0x04 and data_type == 0x00:
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


def handle_zigbee_message(msg):
    """Handle Zigbee2MQTT messages"""
    try:
        device_id = msg.topic.split("/")[-1]
        payload = json.loads(msg.payload.decode())
        
        # Skip non-sensor messages
        if not any(key in payload for key in ['battery', 'temperature', 'action', 'vibration', 'contact']):
            return
        
        # Build fields from numeric values
        fields = {}
        for key, value in payload.items():
            if isinstance(value, (int, float)):
                fields[key] = float(value)
            elif isinstance(value, bool):
                fields[key] = 1.0 if value else 0.0
        
        if fields:
            point = {
                "measurement": "zigbee_sensors",
                "tags": {
                    "device": device_id,
                },
                "fields": fields,
                "time": datetime.now(timezone.utc).isoformat()
            }
            
            influx_client.write_points([point])
            print(f"[Zigbee {device_id[-4:]}] {fields}")
            
    except Exception as e:
        print(f"Zigbee error: {e}")


def on_connect(client, userdata, flags, reason_code, properties=None):
    """MQTT connect callback"""
    print(f"Connected to MQTT broker (rc={reason_code})")
    for topic in MQTT_TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to {topic}")


def on_message(client, userdata, msg):
    """MQTT message callback"""
    try:
        # Zigbee2MQTT messages
        if msg.topic.startswith("zigbee2mqtt/0x"):
            handle_zigbee_message(msg)
            return
        
        # LoRaWAN messages
        payload = json.loads(msg.payload.decode())
        
        device_name = payload.get("deviceName", "unknown")
        application_name = payload.get("applicationName", "unknown")
        dev_eui = payload.get("devEUI", "unknown")
        rssi = payload.get("rxInfo", [{}])[0].get("rssi", 0)
        snr = payload.get("rxInfo", [{}])[0].get("loRaSNR", 0)
        data_base64 = payload.get("data", "")
        
        sensor_values = decode_payload(device_name, application_name, data_base64)
        
        if sensor_values:
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
            
            influx_client.write_points([point])
            print(f"[{device_name}] {sensor_values}")
        
    except Exception as e:
        print(f"Error processing message: {e}")


def main():
    print("NATO IoT Pipeline starting...")
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()