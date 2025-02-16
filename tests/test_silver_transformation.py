import pytest
from pyspark.sql import SparkSession
from silver_operators import TransformBronzeToSilverOperator

# Fixture to create a Spark session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()

# Test for the Silver transformation
def test_transform_to_silver(spark):
    # Simulated input data
    input_data = [
        {
            "id": "1",
            "name": "  BrewCo  ",
            "brewery_type": "MICRO",
            "address_1": "123 Main St",
            "address_2": "",
            "address_3": "",
            "city": "  New York  ",
            "state_province": "NY",
            "country": "USA",
            "longitude": "-74.00597",
            "latitude": "40.71278"
        }
    ]

    # Create an input DataFrame
    input_df = spark.createDataFrame(input_data)

    # Apply the Silver transformation
    transformed_df = TransformBronzeToSilverOperator._transform_to_silver(input_df)

    # Verify the expected columns
    expected_columns = [
        "id", "brewery_name", "brewery_type", "full_address", 
        "city", "state", "country", "longitude", "latitude"
    ]
    assert transformed_df.columns == expected_columns

    # Verify the transformed values
    row = transformed_df.collect()[0]
    assert row["id"] == "1"
    assert row["brewery_name"] == "BrewCo" 
    assert row["brewery_type"] == "micro"  
    assert row["full_address"] == "123 Main St, , "  
    assert row["city"] == "New York"  
    assert row["state"] == "NY"  
    assert row["country"] == "USA"  
    assert row["longitude"] == -74.00597
    assert row["latitude"] == 40.71278  