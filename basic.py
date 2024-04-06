import numpy as np
import pandas as pd

pd_series = pd.Series([10, 20, 30])

print(pd_series)

###

pd_dataframe = pd.DataFrame({
    "Name": ["Jane", "John", "Matt", "Ashley"],
    "Age": [24, 21, 26, 32],
})

print(pd_dataframe)

###

arr = np.random.randint(1, 10, size=(3, 5))
df = pd.DataFrame(arr, columns=["A", "B", "C", "D", "E"])
print(df)
print("size: ", df.size)
print("shape: ", df.shape)
print("len: ", len(df))

###

sales = pd.read_csv("sales.csv", usecols=["product_code", "product_group", "stock_qty"])
print(sales.dtypes)

###

sales = pd.read_csv("sales.csv", usecols=["product_code", "product_group", "stock_qty"])
print(sales.columns)
print(list(sales.columns))

###

sales = pd.read_csv("sales.csv")
sales = sales.astype({
    "stock_qty": "float",
    "last_week_sales": "float"})
print(sales.dtypes)

###
sales = pd.read_csv("sales.csv")
print(sales["product_group"].nunique())
print(sales["product_group"].unique())
print(sales["product_group"].value_counts())
print(sales["price"].var())  # variance
print(sales["price"].std())  # standard deviation

###
myseries = pd.Series([1, 4, 6, 6, 6, 11, 11, 24])
print(myseries.mean())  # 平均
print(myseries.median())  # 中位數
print(myseries.mode()[0])  # 眾數
print(myseries.min())  # 最小值
print(myseries.max())  # 最大值

# practice
sales = pd.read_csv("sales.csv")


def find_most_frequents(column_name):
    try:
        return list(sales[column_name].value_counts().index[0:3])
    except:
        pass


print(find_most_frequents("price"))

###

sales = pd.read_csv("sales.csv")
print(sales.loc[:4, ["product_code", "product_group"]])

print(sales.iloc[[5, 6, 7, 8], [0, 1]])
print(sales.iloc[5:9, :2])

###
df = pd.DataFrame(
    np.random.randint(10, size=(4, 4)),
    index=["a", "b", "c", "d"],
    columns=["col_a", "col_b", "col_c", "col_d"]
)

print(df)

print("\nSelect two rows and two columns using loc:")
print(df.loc[["b", "d"], ["col_a", "col_c"]])

###
sales = pd.read_csv("sales.csv")
selected_columns = ["product_code", "price"]
print(sales[selected_columns].head())

###
sales = pd.read_csv("sales.csv")
sales_filtered_1 = sales[sales["product_group"] == "PG2"]
sales_filtered_2 = sales[sales["price"] > 100]
sales_filtered_3 = sales[(sales["price"] > 100) & (sales["stock_qty"] < 400)]
sales_filtered_4 = sales[(sales["product_group"] == "PG1") | (sales["product_group"] == "PG2")]
sales_filtered_5 = sales[sales["product_group"].isin(["PG1", "PG2", "PG3"])]
sales_filtered_6 = sales[~sales["product_group"].isin(["PG1", "PG2", "PG3"])]  # ~ is not
print(sales["product_group"] == "PG2")
print(sales["product_group"])

###
sales = pd.read_csv("sales.csv")
sales_filtered_1 = sales.query("price > 100")
sales_filtered_2 = sales.query("price > 100 and stock_qty < 400")

###
sales = pd.read_csv("sales.csv")


def find_the_number_of_products():
    average_price = sales["price"].mean()  # find the mean value of the price column
    sales_filtered = sales[
        sales["price"] > average_price]  # filter the products that have a price higher than the average price
    number_of_products = sales_filtered[
        "product_code"].nunique()  # find the number of unique product codes in sales_filtered

    return number_of_products


print(find_the_number_of_products())

###
staff = pd.read_csv("staff.csv")
print(staff)
print(staff["name"].str[0])
print(staff["name"].str[:3])
print(staff["name"].str[1::2])  # str[start : end : step size]
print(staff["name"].str.split(" "))

staff["last_name"] = staff["name"].str.split(" ", expand=True)[1]
print(staff[["name", "last_name"]])

print(staff["name"] + " - " + staff["department"])

staff["name_lower"] = staff["name"].str.lower()
print(staff[["name", "name_lower"]])
print(staff["department"].str.capitalize())

###
staff = pd.read_csv("staff.csv")


def create_city_column():
    staff["state"] = staff["city"].str.split(", ", expand=True)[1]
    return list(staff["state"])


print(create_city_column())

###
staff = pd.read_csv("staff.csv")
print(staff["city"].str.replace(",", "-"))

###
staff = pd.read_csv("staff.csv")
# Create a state colum
staff["state"] = staff["city"].str[-2:]
# Replace state abbreviations with actual state names
staff["state"].replace(
    {"TX": "Texas", "CA": "California", "FL": "Florida", "GA": "Georgia"},
    inplace=True
)
print(staff["state"])

# chained operations
staff = pd.read_csv("staff.csv")
print(staff["city"].str.split(",", expand=True)[1].str.lower())
print(staff["department"].str.lower().replace("field quality", "quality"))
print(staff.query("name > 'John Doe'").start_date.str[:4].astype("int"))

###
staff = pd.read_csv("staff.csv")


def make_salary_proper():
    try:
        staff["salary_cleaned"] = staff["salary"].str[1:].str.replace(",", "")  # Write your solution here
        staff["salary_cleaned"] = staff["salary_cleaned"].astype("int")
        return list(staff["salary_cleaned"])
    except:
        pass


print(make_salary_proper())

###
mydate = pd.to_datetime("2021-11-10")  # datetime64[ns]
print(mydate)

first_date = pd.to_datetime("2021-10-10")
second_date = pd.to_datetime("2021-10-02")
diff = first_date - second_date  # timedelta[ns]
print(diff)
print("\n")
print(diff.days)

###
staff = pd.read_csv("staff.csv")
staff = staff.astype({
    "date_of_birth": "datetime64[ns]",
    "start_date": "datetime64[ns]",
})
print(staff.dtypes)

###
mydate = pd.to_datetime("2021-10-10 14:30:00")
print(f"The year part is {mydate.year}")
print(f"The month part is {mydate.month}")
print(f"The week number part is {mydate.week}")
print(f"The day part is {mydate.day}")
print(f"The hour part of mydate is {mydate.hour}")
print(f"The minute part of mydate is {mydate.minute}")
print(f"The second part of mydate is {mydate.second}")

###
mydate = pd.to_datetime("2021-12-21 00:00:00")
print(f"The date part is {mydate.date()}")
print(f"The day of week is {mydate.weekday()}")
print(f"The name of the month is {mydate.month_name()}")
print(f"The name of the day is {mydate.day_name()}")

###
staff = pd.read_csv("staff.csv")

# change the data type of date columns
staff = staff.astype({
    "date_of_birth": "datetime64[ns]",
    "start_date": "datetime64[ns]",
})

# create start_month column
staff["start_month"] = staff["start_date"].dt.month

print(staff[["start_date", "start_month"]])
print(staff["start_date"].dt.isocalendar())

###
staff = pd.read_csv("staff.csv")
staff = staff.astype({
    "date_of_birth": "datetime64[ns]",
    "start_date": "datetime64[ns]"
})
staff["raise_date"] = staff["start_date"] + pd.DateOffset(years=1)

print(staff[["start_date", "raise_date"]].head())

print(staff["start_date"] + pd.Timedelta(value=12, unit="W"))
print(staff["start_date"] + pd.Timedelta("12 W"))
###
mytime = pd.Timestamp("2021-12-14 16:50:00")
print("The first method")
print(mytime + pd.DateOffset(hours=-2))
print("\nThe second method")
print(mytime - pd.DateOffset(hours=2))

###

staff = pd.read_csv("staff.csv")

staff = staff.astype({
    "date_of_birth": "datetime64[ns]",
    "start_date": "datetime64[ns]"
})


def find_age():
    try:
        staff["age"] = (staff["start_date"] - staff["date_of_birth"]).dt.days / 365
        # convert to integer
        staff["age"] = staff["age"].astype("int")
        return list(staff["age"])
    except:
        pass


print(find_age())

###
df = pd.DataFrame({
    "A": [1, 2, 3, np.nan],
    "B": [2.4, 6.2, 5.1, np.nan],
    "C": ["foo", "zoo", "bar", np.nan]
})
print(df)
print(df.dtypes)

df["A"] = df["A"].astype(pd.Int64Dtype())
print(df)
print(df.dtypes)

###
df = pd.DataFrame({
    "A": [1, 2, 3, np.nan, 7],
    "B": [2.4, np.nan, 5.1, np.nan, 2.6],
    "C": [np.nan, "foo", "zoo", "bar", np.nan],
    "D": [11.5, np.nan, 6.2, 21.1, 8.7]
})
print(df.isna())
print(df.isna().sum())
print(df.isna().sum().sum())

print(df.isna().sum(axis=1))
print(df.notna().sum())

###
df = pd.DataFrame({
    "A": [1, 2, 3, np.nan, 7],
    "B": [2.4, np.nan, 5.1, np.nan, 2.6],
    "C": [np.nan, "foo", "zoo", "bar", np.nan],
    "D": [11.5, np.nan, 6.2, 21.1, 8.7],
    "E": [1, 2, 3, 4, 5]
})
print(df)
print(df.dropna(axis=0, how="any"))
print(df.dropna(axis=1, how="any"))

###
# Drop rows that have less than 4 non-missing values
df.dropna(thresh=4)
print(df)

df.dropna(thresh=4, inplace=True)
print(df)

###
df = pd.DataFrame({
    "A": [1, 2, 3, np.nan, 7],
    "B": [2.4, np.nan, 5.1, np.nan, 2.6],
    "C": [np.nan, "foo", "zoo", "bar", np.nan],
    "D": [11.5, np.nan, 6.2, 21.1, 8.7],
    "E": [1, 2, 3, 4, 5]
})

print(df["A"].fillna(value=df["A"].mean()))

value_a = df["A"].mean()
value_d = df["D"].mean()
print(df.fillna({"A": value_a, "D": value_d}))
