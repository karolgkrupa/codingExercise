from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import operator
from modules.functions import extreme_weather


config = {
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
    "duration": {"q_duration": 5, "e_duration": 3},
}

spark = SparkSession.builder.appName("Extreme weather").getOrCreate()

df = spark.read.parquet("/opt/spark/data/knmi.parquet")
date = df.withColumns(
    {"date": F.to_date("dtg"), "time": F.date_format("dtg", "HH:mm:ss")}
)
cold = extreme_weather(date, **config["cold"], **config["duration"])
hot = extreme_weather(date, **config["hot"], **config["duration"])

cold.write.parquet("/opt/spark/data/cold/")
hot.write.parquet("/opt/spark/data/hot/")