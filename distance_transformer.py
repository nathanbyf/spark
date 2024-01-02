from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, transform, struct, col
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import DecimalType, DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

spark = SparkSession.builder.getOrCreate()
input_path = "transformed_citibike"


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """

    # convert decimal degrees to radians 
    #lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])

    #print(type(map(radians, [lon1, lat1, lon2, lat2])))

    lon1, lat1, lon2, lat2 = radians(float(lon1)), radians(float(lat1)), radians(float(lon2)), radians(float(lat2))

    #lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.

    print(c*r)

    return c * r


#concat_cols = udf(concat, StringType())
haversine_cols = udf(haversine, DecimalType())


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:

    #changedTypedf = dataframe.withColumn("start_station_longitude", dataframe["start_station_longitude"].cast(DecimalType()))

    dataframe = dataframe.na.fill(0)
    
    changedTypedf = dataframe.withColumns({'start_station_longitude': dataframe["start_station_longitude"].cast(DoubleType()) \
                                           ,'start_station_latitude': dataframe["start_station_latitude"].cast(DoubleType()) \
                                           ,'end_station_longitude': dataframe["end_station_longitude"].cast(DoubleType()) \
                                           ,'end_station_latitude': dataframe["end_station_latitude"].cast(DoubleType())
                                           })

    distances = haversine_cols(col("start_station_longitude"), col("start_station_latitude"), col("end_station_longitude"), col("start_station_latitude"))

    print(distances)

    print(type(distances))
    b = changedTypedf.withColumn("distance", distances)

    b.show()

    gg

    # using udf
    #dataframe_distance = changedTypedf.withColumn("distance", haversine_cols(changedTypedf.start_station_longitude, changedTypedf.start_station_latitude, changedTypedf.end_station_longitude, changedTypedf.start_station_latitude))
    dataframe_distance = changedTypedf.withColumn("distance", haversine_cols(col("start_station_longitude"), col("start_station_latitude"), col("end_station_longitude"), col("start_station_latitude")))

    dataframe_distance.select("distance").show()

    gg

    return dataframe_distance


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    #dataset_with_distances.show()

    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')

run(spark, input_path, "test_output")
