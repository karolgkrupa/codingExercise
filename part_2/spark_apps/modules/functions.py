import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.types import DoubleType


def extreme_weather(
    df: DataFrame,
    qualifying: DoubleType,
    extreme: DoubleType,
    comparison,
    col: str,
    q_duration: int,
    e_duration: int,
):
    """
    Identify periods of extreme weather based on consecutive-day temperature thresholds.

    This function detects sustained heat or cold events

    An example of a config is: {
        "hot": {"qualifying": 25, "extreme": 30, "comparison": operator.ge, "col": "maxTemp"},
        "cold": {"qualifying": 0, "extreme": -10, "comparison": operator.le, "col": "minTemp"},
        "duration": {"q_duration": 3, "e_duration": 1}
    },
    which you can then run as
    extreme_weather(df, **config["cold"], **config["duration"])

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing daily temperature observations.
        Expected columns include:
        - date (date)
        - T_DRYB_10 (temperature measurement)
        - Name (location identifier)

    qualifying : float
        Temperature threshold defining a qualifying day
        (e.g., <= 0 for cold events, >= 25 for heat events).

    extreme : float
        Stricter temperature threshold defining an extreme day within
        a qualifying streak (e.g., <= -10 or >= 30).

    comparison : callable
        A comparison function (e.g., operator.lt or operator.gt) used to
        apply the qualifying and extreme thresholds.

    col : str
        Temperature column to evaluate after daily aggregation.
        Permitted values are "minTemp" or "maxTemp".

    q_duration : int
        Minimum number of consecutive qualifying days required to form
        a valid weather event.

    e_duration : int
        Minimum number of extreme-temperature days required within a
        qualifying streak.

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame where each row represents a detected extreme weather event,
        containing:
        - fromDate: start date of the event (inclusive)
        - toDate: end date of the event (inclusive)
        - duration: number of consecutive qualifying days
        - extreme_days_count: number of extreme-temperature days
        - minTemp or maxTemp: maximal or minimal temperature for given wave
    """

    df_temperature = df.groupBy(["date"]).agg(
        F.min("T_DRYB_10").alias("minTemp"), F.max("T_DRYB_10").alias("maxTemp")
    )
    df_filter_temp = df_temperature.filter(comparison(F.col(col), qualifying))
    window = Window.partitionBy(F.lit(1)).orderBy("date")
    df_streak = (
        df_filter_temp.withColumn("prev_date", F.lag("date").over(window))
        .withColumn(
            "is_new_streak",
            F.when(F.datediff(F.col("date"), F.col("prev_date")) != 1, 1).otherwise(0),
        )
        .withColumn("streak_id", F.sum("is_new_streak").over(window))
    )
    df_extreme_temp = (
        df.filter(F.col("Name") == "De Bilt")  # can be parametrized
        .groupBy(["date"])
        .agg(F.min("T_DRYB_10").alias("minTemp"), F.max("T_DRYB_10").alias("maxTemp"))
        .filter(comparison(F.col(col), extreme))
    )
    df_joined = df_streak.alias("hd").join(
        df_extreme_temp.alias("td"), F.col("hd.date") == F.col("td.date"), "left"
    )
    result = (
        df_joined.groupBy("streak_id")
        .agg(
            F.count("hd.*").alias("duration"),
            F.count("td.date").alias("extreme_days_count"),
            F.min("hd.date").alias("fromDate"),
            F.max("hd.date").alias("toDate"),
            F.min("hd.minTemp").alias("minTemp")
            if col == "minTemp"
            else F.max("hd.maxTemp").alias("maxTemp")
            if col == "maxTemp"
            else F.lit(0),
        )
        .filter(
            (F.col("duration") >= q_duration)
            & (F.col("extreme_days_count") >= e_duration)
        )
        .drop("streak_id")
    )
    return result
