This github is used to store various resources for the AnytownUSA project.

Sample Data Generation:
Sample data is generated using a python program and a json file that has water consumption patterns. These patterns are based on actual water consumption data, but do not actually contain any real data. The source data for the patterns is propreitary and cannot be shared, but the provided patterns can be used to generate realistic sample data.

Currently, there is some sample meter water data in the sample_data.csv file

There is also a way to create more sample data using the sythetic-generator-multi.py python file and the patterns.json file as a input.

You need to have python installed with the following libraries: pandas, numpy, json, argparse, datetime, pathlib, and random.

Use the following line in the command line to run python
     water-consumption-generator.py "patterns.json" synthetic_data.csv --num_meters 100 --num_periods 96 --time_interval 15 --start_date 2024-01-01T00:00:00

The synthetic_data.csv is the name of the output file. Keep the time_inverval at 15 to accurately mimic 15 minute intervals. There are 96 15 minute intervals in a day, so use 96 for num_periods to get one day. You can also specific the number of meters to create and the start date.
