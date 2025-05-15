import random
import datetime
import csv
import os
import argparse

def generate_sensor_data(num_records=100, machines=None, anomaly_probability=0.15):
    """
    Generate synthetic sensor data with occasional anomalies
    
    Parameters:
    - num_records: Number of records to generate
    - machines: List of machine IDs (defaults to M001-M010)
    - anomaly_probability: Chance of generating anomalous readings
    
    Returns:
    - List of dictionaries containing sensor data
    """
    if machines is None:
        machines = [f"M{str(i).zfill(3)}" for i in range(1, 11)]
    
    # Base timestamp (current time minus some random offset)
    base_time = datetime.datetime.now() - datetime.timedelta(hours=random.randint(0, 24))
    
    # Normal operating ranges for each sensor
    normal_ranges = {
        "temperature": (65.0, 80.0),  # degrees Celsius
        "vibration": (0.01, 0.2),     # mm/s
        "pressure": (98.0, 102.0)     # kPa
    }
    
    # Warning thresholds
    warning_thresholds = {
        "temperature": 85.0,  # above this is WARNING
        "vibration": 0.5,     # above this is WARNING
        "pressure_high": 105.0,  # above this is WARNING
        "pressure_low": 95.0     # below this is WARNING
    }
    
    # Critical thresholds
    critical_thresholds = {
        "temperature": 95.0,  # above this is CRITICAL
        "vibration": 0.8,     # above this is CRITICAL
        "pressure_high": 110.0,  # above this is CRITICAL
        "pressure_low": 92.0     # below this is CRITICAL
    }
    
    data = []
    
    for i in range(num_records):
        # Select a random machine
        machine_id = random.choice(machines)
        
        # Generate timestamp with some randomness
        timestamp = base_time + datetime.timedelta(
            minutes=random.randint(0, 60),
            seconds=random.randint(0, 59)
        )
        
        # Decide if this reading should be anomalous
        is_anomaly = random.random() < anomaly_probability
        is_critical = is_anomaly and random.random() < 0.3  # 30% of anomalies are critical
        
        # Generate sensor readings
        if is_anomaly:
            if is_critical:
                # Critical anomaly
                anomaly_type = random.choice(["temperature", "vibration", "pressure"])
                
                if anomaly_type == "temperature":
                    temperature = random.uniform(critical_thresholds["temperature"], critical_thresholds["temperature"] + 10)
                    vibration = random.uniform(*normal_ranges["vibration"])
                    pressure = random.uniform(*normal_ranges["pressure"])
                    status = "CRIT"
                elif anomaly_type == "vibration":
                    temperature = random.uniform(*normal_ranges["temperature"])
                    vibration = random.uniform(critical_thresholds["vibration"], critical_thresholds["vibration"] + 0.5)
                    pressure = random.uniform(*normal_ranges["pressure"])
                    status = "CRIT"
                else:  # pressure anomaly
                    temperature = random.uniform(*normal_ranges["temperature"])
                    vibration = random.uniform(*normal_ranges["vibration"])
                    
                    # Either too high or too low pressure
                    if random.random() < 0.5:
                        pressure = random.uniform(critical_thresholds["pressure_high"], critical_thresholds["pressure_high"] + 5)
                    else:
                        pressure = random.uniform(critical_thresholds["pressure_low"] - 5, critical_thresholds["pressure_low"])
                    status = "CRIT"
            else:
                # Warning anomaly
                anomaly_type = random.choice(["temperature", "vibration", "pressure"])
                
                if anomaly_type == "temperature":
                    temperature = random.uniform(warning_thresholds["temperature"], critical_thresholds["temperature"])
                    vibration = random.uniform(*normal_ranges["vibration"])
                    pressure = random.uniform(*normal_ranges["pressure"])
                    status = "WARN"
                elif anomaly_type == "vibration":
                    temperature = random.uniform(*normal_ranges["temperature"])
                    vibration = random.uniform(warning_thresholds["vibration"], critical_thresholds["vibration"])
                    pressure = random.uniform(*normal_ranges["pressure"])
                    status = "WARN"
                else:  # pressure anomaly
                    temperature = random.uniform(*normal_ranges["temperature"])
                    vibration = random.uniform(*normal_ranges["vibration"])
                    
                    # Either too high or too low pressure
                    if random.random() < 0.5:
                        pressure = random.uniform(warning_thresholds["pressure_high"], critical_thresholds["pressure_high"])
                    else:
                        pressure = random.uniform(critical_thresholds["pressure_low"], warning_thresholds["pressure_low"])
                    status = "WARN"
        else:
            # Normal readings
            temperature = random.uniform(*normal_ranges["temperature"])
            vibration = random.uniform(*normal_ranges["vibration"])
            pressure = random.uniform(*normal_ranges["pressure"])
            status = "AOK"
        
        # Format timestamp as ISO format
        timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Create data record
        record = {
            "machine_id": machine_id,
            "timestamp": timestamp_str,
            "temperature": round(temperature, 1),
            "vibration": round(vibration, 2),
            "pressure": round(pressure, 1),
            "status_code": status
        }
        
        data.append(record)
    
    # Sort by timestamp
    data.sort(key=lambda x: x["timestamp"])
    
    return data

def write_csv(data, filename):
    """Write sensor data to CSV file"""
    field_names = ["machine_id", "timestamp", "temperature", "vibration", "pressure", "status_code"]
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Generated {len(data)} records and saved to {filename}")
    
    return filename

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic sensor data")
    parser.add_argument("--records", type=int, default=100, help="Number of records to generate (default: 100)")
    parser.add_argument("--output", type=str, default="data/generated_sensor_data.csv", help="Output CSV file path")
    parser.add_argument("--anomalies", type=float, default=0.15, help="Probability of anomalies (0-1, default: 0.15)")
    
    args = parser.parse_args()
    
    # Generate sensor data
    data = generate_sensor_data(args.records, anomaly_probability=args.anomalies)
    
    # Write to CSV
    output_file = write_csv(data, args.output)
    
    print(f"\nNext steps:")
    print(f"1. Upload the file to S3 to trigger Snowpipe:")
    print(f"   python setup/03_upload_file.py --file {output_file}")
    print(f"2. Check the pipeline status:")
    print(f"   python setup/check_pipeline_status.py") 