# synthetic_generator.py
import pandas as pd
import numpy as np
import json
import argparse
from datetime import datetime, timedelta
import logging
from pathlib import Path
import random

class WaterConsumptionGenerator:
    def __init__(self):
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def load_patterns(self, pattern_file):
        """Load pattern analysis results from JSON file."""
        try:
            with open(pattern_file, 'r') as f:
                patterns = json.load(f)
            self.logger.info(f"Loaded patterns with keys: {list(patterns.keys())}")
            return patterns
        except Exception as e:
            self.logger.error(f"Error loading patterns: {str(e)}")
            raise

    def assign_cluster(self, cluster_probabilities):
        """Assign a cluster based on probability distribution."""
        clusters = list(cluster_probabilities.keys())
        probabilities = list(cluster_probabilities.values())
        return np.random.choice(clusters, p=probabilities)

    def generate_consumption_state(self, current_state, transitions):
        """Generate next consumption state (zero/non-zero) based on transition probabilities."""
        probs = transitions.get(str(current_state), {'0': 0.5, '1': 0.5})
        states = [int(s) for s in probs.keys()]
        probabilities = list(probs.values())
        return np.random.choice(states, p=probabilities)

    def generate_consumption_value(self, gmm_params, temporal_patterns, hour, day_of_week):
        """Generate consumption value using GMM and temporal patterns."""
        try:
            if gmm_params is None:
                return 0

            # Select component based on weights
            component = np.random.choice(len(gmm_params['means']), p=gmm_params['weights'])
            
            # Generate base value from selected component
            base_value = np.random.normal(
                gmm_params['means'][component],
                np.sqrt(gmm_params['covars'][component])
            )
            
            # Get temporal factors with fallback values
            hourly_patterns = temporal_patterns.get('hourly_patterns', {})
            weekly_patterns = temporal_patterns.get('weekly_patterns', {})
            
            hourly_factor = hourly_patterns.get(str(hour), {}).get('mean', 1.0)
            weekly_factor = weekly_patterns.get(str(day_of_week), {}).get('mean', 1.0)
            
            # Add small random variation
            variation = np.random.normal(0, 0.1)
            
            adjusted_value = max(0, base_value * (hourly_factor + weekly_factor) / 2 + variation)
            return round(adjusted_value, 2)
            
        except Exception as e:
            self.logger.error(f"Error generating consumption value: {str(e)}")
            self.logger.error(f"GMM params: {gmm_params}")
            self.logger.error(f"Temporal patterns: {temporal_patterns}")
            self.logger.error(f"Hour: {hour}, Day of week: {day_of_week}")
            return 0

    def generate_meter_data(self, patterns, cluster, start_date, num_periods, time_interval):
        """Generate synthetic data for a single meter."""
        try:
            cluster_patterns = patterns[str(cluster)]
            current_state = 0
            data = []
            
            current_time = start_date
            for _ in range(num_periods):
                hour = current_time.hour
                day_of_week = current_time.weekday()
                
                # Determine if consumption should be zero or non-zero
                current_state = self.generate_consumption_state(
                    current_state,
                    cluster_patterns.get('transitions', {'0': {'0': 0.5, '1': 0.5}})
                )
                
                # Generate consumption value
                if current_state == 0:
                    consumption = 0
                else:
                    consumption = self.generate_consumption_value(
                        cluster_patterns.get('gmm'),
                        cluster_patterns.get('temporal_patterns', {}),
                        hour,
                        day_of_week
                    )
                
                data.append({
                    'TimeStamp': current_time.isoformat(),
                    'Consumption': consumption,
                    'Cluster': int(cluster)
                })
                
                current_time += timedelta(minutes=time_interval)
                
            return data
            
        except Exception as e:
            self.logger.error(f"Error generating meter data: {str(e)}")
            self.logger.error(f"Cluster: {cluster}")
            self.logger.error(f"Patterns keys: {patterns.keys()}")
            raise

    def generate_synthetic_data(self, pattern_file, num_meters, num_periods, 
                              time_interval, start_date, output_file):
        """Generate synthetic data for multiple meters."""
        try:
            self.logger.info("Loading patterns...")
            patterns_data = self.load_patterns(pattern_file)
            
            all_data = []
            start_datetime = datetime.fromisoformat(start_date)
            
            self.logger.info(f"Generating data for {num_meters} meters...")
            for meter_id in range(1, num_meters + 1):
                try:
                    # Assign cluster based on probability distribution
                    cluster = self.assign_cluster(patterns_data['cluster_probabilities'])
                    
                    # Generate data for this meter
                    meter_data = self.generate_meter_data(
                        patterns_data['patterns'],
                        cluster,
                        start_datetime,
                        num_periods,
                        time_interval
                    )
                    
                    # Add meter ID
                    for record in meter_data:
                        record['MeterID'] = meter_id
                    
                    all_data.extend(meter_data)
                    
                    if meter_id % 10 == 0:
                        self.logger.info(f"Generated data for {meter_id} meters...")
                        
                except Exception as e:
                    self.logger.error(f"Error generating data for meter {meter_id}: {str(e)}")
                    continue
            
            # Convert to DataFrame and save
            self.logger.info("Saving generated data...")
            df = pd.DataFrame(all_data)
            df.to_csv(output_file, index=False)
            
            self.logger.info("Data generation complete!")
            
        except Exception as e:
            self.logger.error(f"Error during data generation: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic water consumption data')
    parser.add_argument('pattern_file', type=str, help='Path to pattern analysis JSON file')
    parser.add_argument('output_file', type=str, help='Path to output CSV file')
    parser.add_argument('--num_meters', type=int, default=100, help='Number of meters to simulate')
    parser.add_argument('--num_periods', type=int, default=96, help='Number of time periods per meter')
    parser.add_argument('--time_interval', type=int, default=15, help='Time interval in minutes')
    parser.add_argument('--start_date', type=str, default=datetime.now().isoformat(),
                        help='Start date in ISO format (YYYY-MM-DDTHH:MM:SS)')
    
    args = parser.parse_args()
    
    generator = WaterConsumptionGenerator()
    generator.generate_synthetic_data(
        args.pattern_file,
        args.num_meters,
        args.num_periods,
        args.time_interval,
        args.start_date,
        args.output_file
    )

if __name__ == '__main__':
    main()
