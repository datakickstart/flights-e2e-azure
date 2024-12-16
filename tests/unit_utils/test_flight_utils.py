import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual 
import sys

sys.path.append('./src')

from flights.utils import flight_utils

def test_get_flight_schema__valid():
    schema = flight_utils.get_flight_schema()
    assert schema is not None
    assert len(schema) == 31

def test_cleaned_time_str():
    input = "222"
    result = flight_utils.clean_time_str(input)

    expected = "0222"
    assert result == expected