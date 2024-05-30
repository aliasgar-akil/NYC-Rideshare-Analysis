# NYC Rideshare Analysis

## Overview
This project analyzes NYC rideshare data from Uber and Lyft between January 1, 2023, and May 31, 2023. The dataset was provided by the NYC Taxi and Limousine Commission and is hosted by the state of New York.

## Dataset
- **Source:** NYC Taxi and Limousine Commission Trip Record Data
- **Period:** January 1, 2023, to May 31, 2023
- **Files:**
  - `rideshare_data.csv`
  - `taxi_zone_lookup.csv`

## Tasks
### Task 1: Merging Datasets
- Loaded rideshare and taxi zone lookup data.
- Joined datasets based on pickup and dropoff locations.
- Converted UNIX timestamp to `yyyy-MM-dd` format.
- Verified schema and row count.

### Task 2: Data Aggregation
- Counted trips per business by month.
- Calculated platform profits and driver earnings per business by month.
- Visualized results with histograms.

### Task 3: Top-K Processing
- Identified top 5 popular pickup and dropoff boroughs each month.
- Determined top 30 routes by driver total pay.

### Task 4: Averages Calculation
- Analyzed average driver total pay and trip length by time of day.
- Calculated average earning per mile for different times of day.

### Task 5: Anomalies Detection
- Extracted data for January to calculate average waiting times.
- Identified days with waiting times exceeding 300 seconds and analyzed reasons.

### Task 6: Data Filtering
- Filtered trips by count and analyzed distribution by pickup borough and time of day.
- Counted trips starting in Brooklyn and ending in Staten Island.

### Task 7: Routes Analysis
- Analyzed pickup to dropoff routes to find the top 10 popular routes.

## Tools and Technologies
- **Apache Spark** for large-scale data processing.
- **PySpark** for implementing data analysis tasks.
- **Matplotlib** for visualizing results.

## Results and Insights
- Detailed insights and visualizations can be found in the [report](link_to_report.pdf).
- Key findings and strategic recommendations for stakeholders.

## Repository Structure
- `scripts/`: Contains all Spark scripts for data processing and analysis.
- `data/`: Sample data files used for testing.
- `results/`: Output data files and visualizations.
- `README.md`: This documentation file.

## Getting Started
1. Clone the repository:
    ```sh
    git clone https://github.com/your_username/NYC-Rideshare-Analysis.git
    ```
2. Install necessary dependencies:
    ```sh
    pip install -r requirements.txt
    ```
3. Run the Spark scripts:
    ```sh
    spark-submit scripts/task1.py
    ```

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments
- NYC Taxi and Limousine Commission for providing the dataset.
- Course instructors and resources for guidance on using Spark.
