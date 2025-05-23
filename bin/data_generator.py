import random
import datetime
import csv
import os
import argparse
import glob
import re

def get_latest_batch_info(output_dir="data"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        return 1, None

    batch_files = glob.glob(os.path.join(output_dir, "sensor_data_batch_*.csv"))
    if not batch_files:
        return 1, None

    latest_batch_num = 0
    latest_file = None

    for f in batch_files:
        match = re.search(r"sensor_data_batch_(\d+).csv", os.path.basename(f))
        if match:
            batch_num = int(match.group(1))
            if batch_num > latest_batch_num:
                latest_batch_num = batch_num
                latest_file = f
    
    if latest_file:
        try:
            with open(latest_file, 'r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)
                last_line = None
                for row in reader:
                    if row:
                        last_line = row
                
                if last_line:
                    last_timestamp_str = last_line[1]
                    last_timestamp_dt = datetime.datetime.fromisoformat(last_timestamp_str.replace('Z', '+00:00'))
                    return latest_batch_num + 1, last_timestamp_dt
        except Exception as e:
            print(f"Warning: Could not read or parse last timestamp from {latest_file}: {e}")
            # Fallback if reading fails, start fresh for the next batch number
            return latest_batch_num + 1, datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1) 

    return latest_batch_num + 1, None # Should not happen if latest_file was found, but as a fallback


def generate_sensor_data(
    num_records_per_machine=50, 
    machines=None, 
    anomaly_probability=0.15,
    start_timestamp=None
):
    if machines is None:
        machines = [f"M{str(i).zfill(3)}" for i in range(1, 11)]
    
    num_total_records = num_records_per_machine * len(machines)

    if start_timestamp is None:
        current_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    else:
        current_time = start_timestamp

    normal_ranges = {
        "temperature": (65.0, 80.0),
        "vibration": (0.01, 0.2),
        "pressure": (98.0, 102.0)
    }
    
    warning_thresholds = {
        "temperature": 85.0,
        "vibration": 0.5,
        "pressure_high": 105.0,
        "pressure_low": 95.0
    }
    
    critical_thresholds = {
        "temperature": 95.0,
        "vibration": 0.8,
        "pressure_high": 110.0,
        "pressure_low": 92.0
    }
    
    data = []
    machine_record_counts = {machine_id: 0 for machine_id in machines}
    
    available_machines = list(machines)

    for i in range(num_total_records):
        if not available_machines:
            break 
        
        machine_id = random.choice(available_machines)
        
        current_time += datetime.timedelta(seconds=random.randint(1, 15 + i//10))

        is_anomaly = random.random() < anomaly_probability
        is_critical = is_anomaly and random.random() < 0.3
        
        status = "AOK"
        temperature = random.uniform(*normal_ranges["temperature"])
        vibration = random.uniform(*normal_ranges["vibration"])
        pressure = random.uniform(*normal_ranges["pressure"])

        if is_anomaly:
            anomaly_type = random.choice(["temperature", "vibration", "pressure"])
            if is_critical:
                status = "CRIT"
                if anomaly_type == "temperature":
                    temperature = random.uniform(critical_thresholds["temperature"], critical_thresholds["temperature"] + 10)
                elif anomaly_type == "vibration":
                    vibration = random.uniform(critical_thresholds["vibration"], critical_thresholds["vibration"] + 0.5)
                else: # pressure
                    if random.random() < 0.5:
                        pressure = random.uniform(critical_thresholds["pressure_high"], critical_thresholds["pressure_high"] + 5)
                    else:
                        pressure = random.uniform(critical_thresholds["pressure_low"] - 5, critical_thresholds["pressure_low"])
            else: # Warning
                status = "WARN"
                if anomaly_type == "temperature":
                    temperature = random.uniform(warning_thresholds["temperature"], critical_thresholds["temperature"] -0.1) # ensure it's below critical
                elif anomaly_type == "vibration":
                    vibration = random.uniform(warning_thresholds["vibration"], critical_thresholds["vibration"]-0.01)
                else: # pressure
                    if random.random() < 0.5:
                        pressure = random.uniform(warning_thresholds["pressure_high"], critical_thresholds["pressure_high"]-0.1)
                    else:
                        pressure = random.uniform(critical_thresholds["pressure_low"]+0.1, warning_thresholds["pressure_low"])
        
        timestamp_str = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        record = {
            "machine_id": machine_id,
            "timestamp": timestamp_str,
            "temperature": round(temperature, 1),
            "vibration": round(vibration, 2),
            "pressure": round(pressure, 1),
            "status_code": status
        }
        data.append(record)

        machine_record_counts[machine_id] += 1
        if machine_record_counts[machine_id] >= num_records_per_machine:
            available_machines.remove(machine_id)
            
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
    parser = argparse.ArgumentParser(description="Generate synthetic sensor data in batches.")
    parser.add_argument("--records_per_machine", type=int, default=10, help="Number of records per machine (default: 50)")
    parser.add_argument("--output_dir", type=str, default="data", help="Output directory for CSV files (default: data)")
    parser.add_argument("--anomalies", type=float, default=0.15, help="Probability of anomalies (0-1, default: 0.15)")
    
    args = parser.parse_args()

    next_batch_num, last_timestamp = get_latest_batch_info(args.output_dir)
    
    if last_timestamp:
        print(f"Last timestamp from batch {next_batch_num-1}: {last_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        start_timestamp_for_new_batch = last_timestamp + datetime.timedelta(seconds=random.randint(10,30)) 
    else:
        print("No previous batch found or unable to read last timestamp. Starting fresh.")
        start_timestamp_for_new_batch = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)

    print(f"Generating data for batch {next_batch_num}, starting after ~{start_timestamp_for_new_batch.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    data = generate_sensor_data(
        num_records_per_machine=args.records_per_machine,
        anomaly_probability=args.anomalies,
        start_timestamp=start_timestamp_for_new_batch
    )
    
    output_filename = os.path.join(args.output_dir, f"sensor_data_batch_{next_batch_num}.csv")
    output_file = write_csv(data, output_filename)
