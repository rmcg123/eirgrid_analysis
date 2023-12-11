"""Utility functions for Eirgrid analysis."""
import matplotlib.pyplot as plt
import requests
import pyspark as ps
import seaborn as sns


def get_eirgrid_area_data(base_eirgrid_url, area, start_datetime, end_datetime):
    """Get data from the Eirgrid API."""

    params = {
        "region": "ALL",
        "area": area,
        "datefrom": start_datetime,
        "dateto": end_datetime,
    }
    req = requests.get(
        base_eirgrid_url, params=params
    )
    spark = ps.sql.SparkSession.builder.getOrCreate()

    if req.status_code == 200:
        if req.json()["Status"] != "Error":
            rows = req.json()["Rows"]
            spark_rows = [ps.sql.Row(**x) for x in rows]
            request_data = spark.createDataFrame(data=spark_rows)
        else:
            request_data = spark.createDataFrame(
                [], ps.sql.types.StructType([])
            )
    else:
        request_data = spark.createDataFrame(
            [], ps.sql.types.StructType([])
        )

    return request_data


def clean_eirgrid_data(eirgrid_data):
    """Cleans the retrieved eirgrid data."""

    old_columns = list(eirgrid_data.columns)
    new_columns = []
    for column in old_columns:
        tmp_new_column = column[0].lower() + column[1:]
        new_column = ""
        for char in tmp_new_column:
            if char.isupper():
                new_column += f"_{char.lower()}"
            else:
                new_column += char
        new_columns.append(new_column)

    for old_column, new_column in list(zip(old_columns, new_columns)):

        eirgrid_data = eirgrid_data.withColumnRenamed(old_column, new_column)


    eirgrid_data = eirgrid_data.withColumn(
        "datetime", ps.sql.functions.to_timestamp(
            eirgrid_data["effective_time"], format="dd-MMM-yyyy HH:mm:ss"
        ).alias("datetime")
    )
    eirgrid_data = eirgrid_data.withColumn(
        "value", eirgrid_data["value"].cast("float").alias("value")
    )
    eirgrid_data.printSchema()
    eirgrid_data.show(100)

    eirgrid_data = eirgrid_data.withColumn(
        "month", ps.sql.functions.month(eirgrid_data["datetime"])
    )
    eirgrid_data = eirgrid_data.withColumn(
        "hour", ps.sql.functions.hour(eirgrid_data["datetime"])
    )
    eirgrid_data = eirgrid_data.withColumn(
        "year", ps.sql.functions.year(eirgrid_data["datetime"])
    )

    return eirgrid_data


def plot_boxplot(eirgrid_data, x_col, y_col, area, save_dir, save_name):
    """Function to produce a simple boxplot showing some selected
    eirgrid data."""

    fig, ax = plt.subplots()

    sns.boxplot(
        data=eirgrid_data,
        x=x_col,
        y=y_col,
        orient="h",
    )

    if area in ["wind_generation", "demand", "generation", "interconnection"]:
        units = "MW"
    elif area == "snsp":
        units = "%"
    else:
        units = "g CO2/kWh"
    area = area.replace("_", " ")

    ax.set_title(f"{area.title()} by {y_col.title()}")
    ax.set_xlabel(f"{area.title()}, {units}")
    ax.set_ylabel(f"{y_col.title()}")

    fig.savefig(save_dir + save_name, bbox_inches="tight")

    return fig, ax
