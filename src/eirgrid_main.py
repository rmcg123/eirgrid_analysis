"""Main script for eirgrid analysis."""
import os
import time

import pyspark as ps
import pandas as pd
from matplotlib import rcParams

import src.eirgrid_functions as ef
import src.eirgrid_config as cfg

rcParams["font.family"] = "Arial"
rcParams["figure.figsize"] = (16, 9)
rcParams["figure.dpi"] = 300
rcParams["axes.titlesize"] = 24
rcParams["axes.labelsize"] = 18
rcParams["font.size"] = 16
rcParams["xtick.labelsize"] = 16
rcParams["ytick.labelsize"] = 14
rcParams["legend.fontsize"] = 12
rcParams["legend.title_fontsize"] = 16

def get_data(area, years, data_folder):
    """Gets, saves and returns eirgrid data from a particular area for all days
     within a list of provided years."""

    spark = ps.sql.SparkSession.builder.getOrCreate()
    # Try to get the data for all years for the chosen area if it's already
    # been retrieved.
    try:
        years_df = spark.read.csv(
            data_folder + f"{area}/all.csv", header=True
        )

    # Otherwise go through each year and get the data for that area for each
    # day of each year.
    except ps.errors.AnalysisException:

        for idx, year in enumerate(years):

            # Set up folders and start and end dates.
            print("Getting data for", area, "for year", year)
            year_dir = data_folder + f"{area}/{year}/"
            os.makedirs(year_dir, exist_ok=True)
            if year != "2023":
                end_date = f"{year}-12-31"
            else:
                end_date = "2023-11-30"
            dates = pd.date_range(start=f"{year}-01-01", end=end_date).to_list()

            # Loop through each date within year.
            dates_dir = year_dir + "dates/"
            os.makedirs(dates_dir, exist_ok=True)
            for jdx, date in enumerate(reversed(dates)):
                print("Getting data for area", area, "date", date)

                # Try and see if data for that date has already been retrieved,
                # otherwise retrieve it.
                try:
                    file_name = str(date).replace(":", "-") + ".csv"
                    date_df = spark.read.csv(
                        dates_dir + file_name, header=True
                    )
                    print("Loaded date df from file")
                except ps.errors.AnalysisException:
                    start_datetime = date.strftime(
                        format="%d-%b-%Y"
                    ) + " 00:00"
                    end_datetime = date.strftime(format="%d-%b-%Y") + " 23:59"
                    date_df = ef.get_eirgrid_area_data(
                        base_eirgrid_url=cfg.EIRGRID_API_URL,
                        area=area,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                    )
                    date = str(date).replace(":", "-")
                    date_df.toPandas().to_csv(dates_dir + f"{date}.csv")
                    if date_df.count() > 0:
                        time.sleep(1)

                if jdx == 0:
                    year_df = date_df
                elif len(date_df.columns) == len(year_df.columns):
                    # Add data for this date to accruing DataFrame for this year.
                    year_df = year_df.unionAll(date_df)
                else:
                    continue

            # Once year completed save DataFrame for this year. Add year
            # DataFrame to accruing multi-year DataFrame.
            year_df.toPandas().to_csv(year_dir + f"{year}.csv")
            if idx == 0:
                years_df = year_df
            elif len(year_df.columns) == (years_df.columns):
                years_df = years_df.unionAll(year_df)
            else:
                continue

        # Save multi-year DataFrame.
        years_df.toPandas().to_csv(data_folder + f"{area}/all.csv")

    return years_df

def main():

    # Loop over each data area and add DataFrame to dictionary.
    data_dict = {}
    for area, area_name in cfg.DATA_AREAS.items():

        print(area)
        # Get data for this area.
        data_dict[area] = get_data(
            area=area_name,
            years=cfg.YEARS,
            data_folder=cfg.DATA_FOLDER,
        )

        # Add to dictionary.
        data_dict[area] = ef.clean_eirgrid_data(data_dict[area])

        for y_col in ["year", "month", "hour"]:
            save_dir = cfg.RESULTS_FOLDER + f"{area}/"
            os.makedirs(save_dir, exist_ok=True)

            data = data_dict[area].toPandas()

            _, _ = ef.plot_boxplot(
                data,
                x_col="value",
                y_col=y_col,
                save_dir=save_dir,
                save_name=f"{y_col}.png",
                area=area
            )


if __name__ == "__main__":
    main()
