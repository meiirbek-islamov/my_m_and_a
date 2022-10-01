import sqlite3
import pandas as pd
import csv
from pathlib import Path

def my_m_and_a(content_database_1, content_database_2, content_database_3, db_name):
    Path(db_name).touch()
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    #CSV1
    df1 = pd.read_csv(content_database_1)

    # Cleaning up the CSV2
    df2 = pd.read_csv(content_database_2, sep=';', header=None)
    df2.columns = (['Age', 'City', 'Gender', 'Name', 'Email'])
    new = df2.Name.str.split(expand = True)
    df2["FirstName"] = new[0]
    df2["LastName"] = new[1]
    df2.drop(columns="Name", inplace=True)
    df2_reorderd_columns = ['Gender', 'FirstName', 'LastName', 'Email', 'Age', 'City']
    df2 = df2[df2_reorderd_columns]

    # Cleaning up the CSV3
    df3 = pd.read_csv(content_database_3)
    df3_col_names = df3.columns
    df3 = df3.Gender.str.split("\t", expand = True)
    df3.columns = df3_col_names
    for col in df3:
        if col == "Gender":
            df3[col] = df3[col].str.split("_", expand = True)[1]
        if col == "Age":
            df3[col] = df3[col].str.split("_", expand = True)[1].str.split('"', expand = True)[0]
        if col == "Name" or col == "Email" or col == "City" or col == "Country":
            df3[col] = df3[col].str.split("string_", expand = True)[1]
    new3 = df3.Name.str.split(expand = True)
    df3["FirstName"] = new[0]
    df3["LastName"] = new[1]
    df3.drop(columns="Name", inplace=True)
    df3_reorderd_columns = ['Gender', 'FirstName', 'LastName', 'Email', 'Age', 'City', 'Country']
    df3 = df3[df3_reorderd_columns]

    # Concatenate three DataFrames into one
    frames = [df1, df2, df3]
    result = pd.concat(frames)

    column_names = tuple([col for col in result.columns])

    con.commit()
    result.to_sql("customers", con, if_exists='fail', index = False)
    con.close()

my_m_and_a("only_wood_customer_us_1.csv", "only_wood_customer_us_2.csv", "only_wood_customer_us_3.csv", 'plastic_free_boutique.sql')

# Remove the csv files after integrating them into the database
cwd = os.getcwd()
file1, file2, file3 = "only_wood_customer_us_1.csv", "only_wood_customer_us_2.csv", "only_wood_customer_us_3.csv"
path1 = os.path.join(cwd, file1)
path2 = os.path.join(cwd, file2)
path3 = os.path.join(cwd, file3)
os.remove(path1)
os.remove(path2)
os.remove(path3)