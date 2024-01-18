import pandas as pd

#########
#
# Using Pandas, we'll create two basic DataFrames and merge them together
#
#########

# Open orders CSV into pandas dataframe
orders_df = pd.read_csv("./orders.csv")

# Open employees CSV into pandas dataframe
emps_df = pd.read_csv("./employees.csv")

# Use pandas .merge() to join dataframes
combined_df = pd.merge(
    orders_df, emps_df, how="inner", left_on="employee_id", right_on="id"
)

# Drop the existing 'id' columns
combined_df.drop(["id_y", "id_x"], axis=1, inplace=True)

# Add row num/id columns
combined_df["id"] = combined_df.index + 1

# Move id column to first position
combined_df.insert(0, "id", combined_df.pop("id"))


#########
#
# Now we'll use the Tableau Hyper API to convert the frames into a .hyper file
#
#########

# Using pandas + pantab:
# This is definitely the easiest/fastest method, but lacks some of the
# customization options
#
#
# https://tableau.github.io/hyper-db/docs/guides/pandas_integration/#loading-data-through-pandas

import pantab

pantab.frame_to_hyper(combined_df, "./order_data.hyper", table="Orders")


# Using tableauhyperapi:
# This is definitely the easiest/fastest method, but lacks some of the
# customization options
#
#
# https://tableau.github.io/hyper-db/docs/guides/hyper_file/create_update

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    TableDefinition,
    TableName,
    SqlType,
    Inserter,
)

hyper_cols = []

for col in dict(combined_df.dtypes).keys():
    hyper_cols.append(TableDefinition.Column(col, SqlType.text()))

with HyperProcess(
    Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters={"default_database_version": "2"}
) as hyper:
    with Connection(
        hyper.endpoint, "orders_hyper_api.hyper", CreateMode.CREATE_AND_REPLACE
    ) as connection:
        connection.catalog.create_schema("Orders")
        example_table = TableDefinition(TableName("Orders", "Orders"), hyper_cols)

        connection.catalog.create_table(example_table)

        with Inserter(connection, example_table) as inserter:
            for index, row in combined_df.iterrows():
                inserter.add_row([str(val) for val in dict(row).values()])

            inserter.execute()
