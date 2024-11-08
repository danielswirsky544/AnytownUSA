# synthetic_generator.py
import pandas as pd
import numpy as np
import json
import argparse
from datetime import datetime
from scipy.stats import norm
from typing import List, Dict, Union
import concurrent.futures
import time

def generate_consumption_value(pattern, previous_value):
    """Generate a realistic consumption value based on patterns."""
    # Determine if this should be a zero or non-zero value
    is_currently_zero = previous_value == 0
    random_val = np.random.random()
    
    if is_currently_zero:
        if random_val < pattern['transitions']['zero_to_non_zero']:
            should_be_zero = False
        else:
            should_be_zero = True
    else:
        if random_val < pattern['transitions']['non_zero_to_zero']:
            should_be_zero = True
        else:
            should_be_zero = False
    
    if should_be_zero:
        return 0
    
    # Generate non-zero value using mixture model
    component = np.random.choice(len(pattern['gmm_means']), p=pattern['gmm_weights'])
    value = np.random.normal(
        pattern['gmm_means'][component],
        np.sqrt(pattern['gmm_covariances'][component])
    )
    
    # Ensure value is within reasonable bounds
    value = max(pattern['basic_stats']['min'], 
               min(pattern['basic_stats']['max'], value))
    
    # Occasionally use common values for more realistic patterns
    if np.random.random() < 0.2:  # 20% chance to use common value
        value = pattern['common_values']
    
    return max(0, value)

def generate_meter_data(patterns: Dict, 
                       start_date: str, 
                       end_date: str, 
                       object_id: int,
                       variation_factor: float = 0.1) -> pd.DataFrame:
    """Generate synthetic data for a single meter."""
    date_range = pd.date_range(start=start_date, end=end_date, freq='15min')
    synthetic_data = []
    previous_value = 0
    
    # Add small random variation to patterns for this specific meter
    meter_patterns = {}
    for day in patterns.keys():
        if day == 'global_patterns':
            continue
        meter_patterns[day] = {}
        for time, time_pattern in patterns[day].items():
            if time == 'daily_sequence':
                meter_patterns[day][time] = time_pattern
                continue
                
            # Add slight variations to the patterns for each meter
            varied_pattern = {
                'gmm_means': [m * (1 + np.random.normal(0, variation_factor)) 
                             for m in time_pattern['gmm_means']],
                'gmm_weights': time_pattern['gmm_weights'],
                'gmm_covariances': time_pattern['gmm_covariances'],
                'percentiles': time_pattern['percentiles'],
                'common_values': time_pattern['common_values'] * 
                               (1 + np.random.normal(0, variation_factor)),
                'transitions': time_pattern['transitions'],
                'zero_probability': time_pattern['zero_probability'],
                'basic_stats': {
                    'mean': time_pattern['basic_stats']['mean'] * 
                           (1 + np.random.normal(0, variation_factor)),
                    'std': time_pattern['basic_stats']['std'],
                    'min': time_pattern['basic_stats']['min'],
                    'max': time_pattern['basic_stats']['max']
                }
            }
            meter_patterns[day][time] = varied_pattern

    for timestamp in date_range:
        day = timestamp.day_name()
        time = timestamp.strftime('%H:%M')
        hour = timestamp.hour
        
        # Get pattern for this day and time
        day_patterns = meter_patterns.get(day, {})
        time_pattern = day_patterns.get(time, {})
        
        if time_pattern and 'daily_sequence' in day_patterns:
            # Check if this is typically a peak or zero period
            is_peak_hour = hour in day_patterns['daily_sequence']['peak_times']
            is_zero_hour = hour in day_patterns['daily_sequence']['zero_periods']
            
            # Adjust probability based on typical patterns
            if is_peak_hour:
                consumption = generate_consumption_value(time_pattern, previous_value)
                # Increase likelihood of non-zero values during peak hours
                if consumption == 0 and np.random.random() < 0.7:
                    consumption = generate_consumption_value(time_pattern, previous_value)
            elif is_zero_hour:
                consumption = 0 if np.random.random() < 0.8 else generate_consumption_value(time_pattern, previous_value)
            else:
                consumption = generate_consumption_value(time_pattern, previous_value)
            
            # Apply some random variation to maintain realistic patterns
            if consumption > 0:
                variation = np.random.normal(0, time_pattern['basic_stats']['std'] * 0.1)
                consumption = max(0, consumption + variation)
            
            previous_value = consumption
        else:
            consumption = 0
            
        synthetic_data.append({
            'OBJECTID': object_id,
            'TimeStamp': timestamp,
            'Consumption': round(consumption, 2)
        })
    
    return pd.DataFrame(synthetic_data)

def generate_synthetic_data(patterns_path: str, 
                          start_date: str, 
                          end_date: str, 
                          output_path: str, 
                          num_meters: int = 1,
                          base_object_id: int = 1,
                          variation_factor: float = 0.1) -> pd.DataFrame:
    """Generate synthetic water consumption data for multiple meters."""
    print(f"Loading patterns from {patterns_path}...")
    with open(patterns_path, 'r') as f:
        patterns = json.load(f)
    
    print(f"Generating synthetic data for {num_meters} meters from {start_date} to {end_date}...")
    start_time = time.time()
    
    # Generate data for multiple meters in parallel
    all_data = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_meter = {
            executor.submit(
                generate_meter_data, 
                patterns, 
                start_date, 
                end_date, 
                base_object_id + i,
                variation_factor
            ): i for i in range(num_meters)
        }
        
        # Show progress
        completed = 0
        for future in concurrent.futures.as_completed(future_to_meter):
            meter_num = future_to_meter[future] + 1
            try:
                meter_data = future.result()
                all_data.append(meter_data)
                completed += 1
                print(f"Generated data for meter {meter_num}/{num_meters} "
                      f"({(completed/num_meters)*100:.1f}% complete)")
            except Exception as e:
                print(f"Error generating data for meter {meter_num}: {str(e)}")
    
    # Combine all meter data
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by TimeStamp and OBJECTID
    final_df = final_df.sort_values(['TimeStamp', 'OBJECTID'])
    
    print(f"Saving synthetic data to {output_path}...")
    final_df.to_csv(output_path, index=False)
    
    print(f"\nSynthetic data generation complete!")
    print(f"Total time: {time.time() - start_time:.1f} seconds")
    print(f"Generated {len(final_df)} records for {num_meters} meters")
    
    # Print some basic statistics
    print("\nData Summary:")
    print(f"Date range: {final_df['TimeStamp'].min()} to {final_df['TimeStamp'].max()}")
    print("Average consumption by meter:")
    meter_stats = final_df.groupby('OBJECTID')['Consumption'].agg(['mean', 'max', 'min'])
    print(meter_stats.round(2))
    
    return final_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate synthetic water consumption data')
    parser.add_argument('patterns_file', help='Path to patterns JSON file')
    parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', default='synthetic_consumption_data.csv',
                      help='Path to output CSV file (default: synthetic_consumption_data.csv)')
    parser.add_argument('--num-meters', type=int, default=1,
                      help='Number of meters to generate (default: 1)')
    parser.add_argument('--base-object-id', type=int, default=1,
                      help='Starting object ID for meters (default: 1)')
    parser.add_argument('--variation-factor', type=float, default=0.1,
                      help='Variation factor between meters (0-1, default: 0.1)')
    
    args = parser.parse_args()
    
    try:
        synthetic_data = generate_synthetic_data(
            args.patterns_file,
            args.start_date,
            args.end_date,
            args.output,
            args.num_meters,
            args.base_object_id,
            args.variation_factor
        )
        print(f"Successfully generated synthetic data and saved to {args.output}")
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)
