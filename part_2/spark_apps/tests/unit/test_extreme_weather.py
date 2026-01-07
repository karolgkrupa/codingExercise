from datetime import datetime
import os, sys, operator

from pytest import fixture

# from pyspark.testing import assertDataFrameEqual # Apparently doesn't work in this spark version! - https://community.databricks.com/t5/data-engineering/want-to-use-dataframe-equality-functions-but-also-numpy-gt-2-0/m-p/138009/highlight/true#M50845
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from modules.functions import extreme_weather


@fixture(scope="session")
def config():
    return {
        "hot": {
            "qualifying": 25,
            "extreme": 30,
            "comparison": operator.ge,
            "col": "maxTemp",
        },
        "cold": {
            "qualifying": 0,
            "extreme": -10,
            "comparison": operator.le,
            "col": "minTemp",
        },
        "duration": {"q_duration": 3, "e_duration": 1},
    }


def test_hot_weather(spark, config):
    init_data = spark.createDataFrame(
        [
            {"name": "De Bilt", "T_DRYB_10": 25, "date": datetime(2020, 1, 11).date()},
            {"name": "De Bilt", "T_DRYB_10": 30, "date": datetime(2020, 1, 12).date()},
            {"name": "De Bilt", "T_DRYB_10": 28, "date": datetime(2020, 1, 13).date()},
            {"name": "De Bilt", "T_DRYB_10": 20, "date": datetime(2020, 1, 14).date()},
        ]
    )

    df_actual = (
        extreme_weather(init_data, **config["hot"], **config["duration"])
        .select("duration", "extreme_days_count", "fromDate", "toDate", "maxTemp")
        .orderBy("fromDate")
    )

    df_expected = (
        spark.createDataFrame(
            [
                {
                    "duration": 3,
                    "extreme_days_count": 1,
                    "fromDate": datetime(2020, 1, 11).date(),
                    "toDate": datetime(2020, 1, 13).date(),
                    "maxTemp": 30,
                }
            ]
        )
        .select("duration", "extreme_days_count", "fromDate", "toDate", "maxTemp")
        .orderBy("fromDate")
    )

    assert df_actual.collect() == df_expected.collect()


def test_cold_weather(spark, config):
    init_data = spark.createDataFrame(
        [
            {"name": "De Bilt", "T_DRYB_10": 0, "date": datetime(2020, 1, 11).date()},
            {"name": "De Bilt", "T_DRYB_10": -10, "date": datetime(2020, 1, 12).date()},
            {"name": "De Bilt", "T_DRYB_10": 0, "date": datetime(2020, 1, 13).date()},
            {"name": "De Bilt", "T_DRYB_10": 20, "date": datetime(2020, 1, 14).date()},
        ]
    )

    df_actual = (
        extreme_weather(init_data, **config["cold"], **config["duration"])
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    df_expected = (
        spark.createDataFrame(
            [
                {
                    "duration": 3,
                    "extreme_days_count": 1,
                    "fromDate": datetime(2020, 1, 11).date(),
                    "toDate": datetime(2020, 1, 13).date(),
                    "minTemp": -10,
                }
            ]
        )
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    assert df_actual.collect() == df_expected.collect()


def test_streaks_with_null(spark, config):
    init_data = spark.createDataFrame(
        [
            {"name": "De Bilt", "T_DRYB_10": 0, "date": datetime(2020, 1, 11).date()},
            {"name": "De Bilt", "T_DRYB_10": -10, "date": datetime(2020, 1, 12).date()},
            {"name": "De Bilt", "T_DRYB_10": 0, "date": datetime(2020, 1, 13).date()},
            {"name": "De Bilt", "T_DRYB_10": 20, "date": datetime(2020, 1, 14).date()},
            {"name": "De Bilt", "T_DRYB_10": -3, "date": datetime(2020, 1, 15).date()},
            {"name": "De Bilt", "T_DRYB_10": -5, "date": datetime(2020, 1, 16).date()},
            {"name": "De Bilt", "T_DRYB_10": -8, "date": datetime(2020, 1, 17).date()},
            {"name": "De Bilt", "T_DRYB_10": -15, "date": datetime(2020, 1, 18).date()},
            {"name": "De Bilt", "T_DRYB_10": -20, "date": datetime(2020, 1, 19).date()},
            {
                "name": "De Bilt",
                "T_DRYB_10": None,
                "date": datetime(2020, 1, 20).date(),
            },
            {"name": "De Bilt", "T_DRYB_10": 0, "date": datetime(2020, 1, 21).date()},
        ]
    )

    df_actual = (
        extreme_weather(init_data, **config["cold"], **config["duration"])
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    df_expected = (
        spark.createDataFrame(
            [
                {
                    "duration": 3,
                    "extreme_days_count": 1,
                    "fromDate": datetime(2020, 1, 11).date(),
                    "toDate": datetime(2020, 1, 13).date(),
                    "minTemp": -10,
                },
                {
                    "duration": 5,
                    "extreme_days_count": 2,
                    "fromDate": datetime(2020, 1, 15).date(),
                    "toDate": datetime(2020, 1, 19).date(),
                    "minTemp": -20,
                },
            ]
        )
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    assert df_actual.collect() == df_expected.collect()


def test_different_location(spark, config):
    init_data = spark.createDataFrame(
        [
            {"name": "Miami", "T_DRYB_10": 0, "date": datetime(2020, 1, 11).date()},
            {"name": "Miami", "T_DRYB_10": -10, "date": datetime(2020, 1, 12).date()},
            {"name": "Miami", "T_DRYB_10": 0, "date": datetime(2020, 1, 13).date()},
            {"name": "Miami", "T_DRYB_10": 20, "date": datetime(2020, 1, 14).date()},
            {"name": "Miami", "T_DRYB_10": 0, "date": datetime(2020, 1, 15).date()},
            {"name": "De Bilt", "T_DRYB_10": -10, "date": datetime(2020, 1, 16).date()},
            {"name": "Miami", "T_DRYB_10": 0, "date": datetime(2020, 1, 17).date()},
        ]
    )

    # As long as extreme weather has name "De Bilt" location on other rows doesn't matter

    df_actual = (
        extreme_weather(init_data, **config["cold"], **config["duration"])
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    df_expected = (
        spark.createDataFrame(
            [
                {
                    "duration": 3,
                    "extreme_days_count": 1,
                    "fromDate": datetime(2020, 1, 15).date(),
                    "toDate": datetime(2020, 1, 17).date(),
                    "minTemp": -10,
                }
            ]
        )
        .select("duration", "extreme_days_count", "fromDate", "toDate", "minTemp")
        .orderBy("fromDate")
    )

    assert df_actual.collect() == df_expected.collect()
