import duckdb
import logging
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

# Paths to CSVs (adjust if needed)
YELLOW_CSV = Path("output/yellow_trips.csv")
GREEN_CSV = Path("output/green_trips.csv")
OUTPUT_PLOT = Path("output/co2_by_month.png")

# Helper mapping from DuckDB dayofweek (Sunday=0) to names
DAYNAME = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday",
           4: "Thursday", 5: "Friday", 6: "Saturday"}

def load_and_clean(path):
    logger.info("""Loading CSV into a DataFrame and performing basic cleaning / dtype fixes.""")
    df = pd.read_csv(path, parse_dates=["pickup_datetime", "dropoff_datetime"], infer_datetime_format=True)

    # Ensure expected columns exist
    expected = {"pickup_datetime", "dropoff_datetime", "trip_distance", "trip_co2_kgs",
                "duration_minutes", "avg_mph", "hour_of_day", "day_of_week", "week_of_year", "month_of_year"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns in {path}: {missing}")

    # Coerce numeric types
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["trip_co2_kgs"] = pd.to_numeric(df["trip_co2_kgs"], errors="coerce")
    df["duration_minutes"] = pd.to_numeric(df["duration_minutes"], errors="coerce")
    df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce").astype("Int64")
    df["day_of_week"] = pd.to_numeric(df["day_of_week"], errors="coerce").astype("Int64")
    df["week_of_year"] = pd.to_numeric(df["week_of_year"], errors="coerce").astype("Int64")
    df["month_of_year"] = pd.to_numeric(df["month_of_year"], errors="coerce").astype("Int64")

    # Drop rows missing essential values
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "trip_distance", "trip_co2_kgs"])

    # Filter out non-positive distances or durations (as per model filtering, but safeguard)
    df = df[(df["trip_distance"] > 0) & (df["duration_minutes"] > 0) & (df["trip_co2_kgs"] >= 0)]
    logger.info("Complete")
    return df

def largest_trip(df):
    logger.info("""Returning the row (as Series) corresponding to the single largest trip_co2_kgs.""")
    idx = df["trip_co2_kgs"].idxmax()
    logger.info("Complete")
    return df.loc[idx]

def avg_by_group(df, group_col, value_col="trip_co2_kgs"):
    logger.info("""Calculating average by group.""")
    s = df.groupby(group_col)[value_col].mean()
    # Ensure numeric sorted index
    logger.info("Complete")
    return s.sort_index()

def print_extreme(series, label, value_formatter=lambda v: f"{v:.3f}"):
    logger.info("""Printing top and bottom single values from a series (index -> value).""")
    top_idx = series.idxmax()
    bottom_idx = series.idxmin()
    logger.info(f"  {label} — Highest: {top_idx} ({value_formatter(series.loc[top_idx])}), Lowest: {bottom_idx} ({value_formatter(series.loc[bottom_idx])})")

def analyze_one(df, label):
    logger.info(f"\n=== Analysis for {label} trips ===")
    # Largest trip
    largest = largest_trip(df)
    logger.info("Single largest carbon-producing trip (by trip_co2_kgs):")
    logger.info(f"  pickup: {largest['pickup_datetime']}")
    logger.info(f"  dropoff: {largest['dropoff_datetime']}")
    logger.info(f"  trip_distance: {largest['trip_distance']}")
    logger.info(f"  trip_co2_kgs: {largest['trip_co2_kgs']:.6f}")
    logger.info(f"  duration_minutes: {largest['duration_minutes']}")
    logger.info()

    # Avg by hour_of_day (0-23)
    hour_avg = avg_by_group(df, "hour_of_day")
    # Convert Int64 index to normal ints for printing
    hour_avg_idx = hour_avg.index.astype(int)
    top_hour = int(hour_avg_idx[hour_avg.argmax()])
    bottom_hour = int(hour_avg_idx[hour_avg.argmin()])
    logger.info("Average trip CO2 per hour of day (0=midnight):")
    logger.info(f"  Most carbon-heavy hour: {top_hour} (avg CO2 {hour_avg.max():.6f} kg/trip)")
    logger.info(f"  Least carbon-heavy hour: {bottom_hour} (avg CO2 {hour_avg.min():.6f} kg/trip)")
    logger.info()

    # Avg by day_of_week (DuckDB: Sunday=0)
    dow_avg = avg_by_group(df, "day_of_week")
    dow_idx = dow_avg.index.astype(int)
    top_dow = int(dow_idx[dow_avg.argmax()])
    bottom_dow = int(dow_idx[dow_avg.argmin()])
    logger.info("Average trip CO2 per day of week:")
    logger.info(f"  Most carbon-heavy day: {DAYNAME.get(top_dow, str(top_dow))} (avg CO2 {dow_avg.max():.6f} kg/trip)")
    logger.info(f"  Least carbon-heavy day: {DAYNAME.get(bottom_dow, str(bottom_dow))} (avg CO2 {dow_avg.min():.6f} kg/trip)")
    logger.info()

    # Avg by week_of_year (1-52)
    week_avg = avg_by_group(df, "week_of_year")
    week_idx = week_avg.index.astype(int)
    top_week = int(week_idx[week_avg.argmax()])
    bottom_week = int(week_idx[week_avg.argmin()])
    logger.info("Average trip CO2 per week of year:")
    logger.info(f"  Most carbon-heavy week: {top_week} (avg CO2 {week_avg.max():.6f} kg/trip)")
    logger.info(f"  Least carbon-heavy week: {bottom_week} (avg CO2 {week_avg.min():.6f} kg/trip)")
    logger.info()

    # Avg by month_of_year (1-12)
    month_avg = avg_by_group(df, "month_of_year")
    month_idx = month_avg.index.astype(int)
    top_month = int(month_idx[month_avg.argmax()])
    bottom_month = int(month_idx[month_avg.argmin()])
    import calendar
    logger.info("Average trip CO2 per month:")
    logger.info(f"  Most carbon-heavy month: {calendar.month_name[top_month]} (avg CO2 {month_avg.max():.6f} kg/trip)")
    logger.info(f"  Least carbon-heavy month: {calendar.month_name[bottom_month]} (avg CO2 {month_avg.min():.6f} kg/trip)")
    logger.info()

    # Return aggregates for plotting: monthly totals
    monthly_totals = df.groupby("month_of_year")["trip_co2_kgs"].sum().reindex(range(1,13), fill_value=0)
    return monthly_totals

def plot_monthly(yellow_monthly, green_monthly, out_path):
    """Plot MONTH (1..12) vs CO2 totals for yellow and green and save to file."""
    plt.figure(figsize=(10,6))
    months = np.arange(1,13)
    # If the series index isn't exactly 1..12, reindex
    y = yellow_monthly.reindex(months, fill_value=0).values
    g = green_monthly.reindex(months, fill_value=0).values

    # Plot lines
    plt.plot(months, y, marker='o', label='YELLOW total CO2 (kg)')
    plt.plot(months, g, marker='o', label='GREEN total CO2 (kg)')

    # X-axis ticks: use month names
    import calendar
    month_names = [calendar.month_abbr[m] for m in months]
    plt.xticks(months, month_names)
    plt.xlabel("Month")
    plt.ylabel("Total CO2 (kg)")
    plt.title("Monthly total CO2 from taxi trips — Yellow vs Green")
    plt.grid(axis='y', linestyle='--', linewidth=0.5)
    plt.legend()
    plt.tight_layout()

    # Save
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=300)
    plt.close()
    logger.info(f"Saved monthly CO2 plot to: {out_path}")

def main():
    if not YELLOW_CSV.exists():
        raise FileNotFoundError(f"{YELLOW_CSV} not found. Run dbt to generate the CSV first.")
    if not GREEN_CSV.exists():
        raise FileNotFoundError(f"{GREEN_CSV} not found. Run dbt to generate the CSV first.")

    yellow_df = load_and_clean(YELLOW_CSV)
    green_df = load_and_clean(GREEN_CSV)

    yellow_monthly = analyze_one(yellow_df, "YELLOW")
    green_monthly = analyze_one(green_df, "GREEN")

    logger.info("\n=== Monthly totals (kg CO2) — sample output ===")
    combined = pd.DataFrame({
        "month": range(1,13),
        "yellow_total_kg": yellow_monthly.reindex(range(1,13), fill_value=0).values,
        "green_total_kg": green_monthly.reindex(range(1,13), fill_value=0).values
    })
    logger.info(combined)

    # Plot and save
    plot_monthly(yellow_monthly, green_monthly, OUTPUT_PLOT)

if __name__ == "__main__":
    main()