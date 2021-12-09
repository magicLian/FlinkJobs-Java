#!/usr/bin/env python3

from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.INT()], result_type=DataTypes.INT())
def add_one(a):
    return a + 1




