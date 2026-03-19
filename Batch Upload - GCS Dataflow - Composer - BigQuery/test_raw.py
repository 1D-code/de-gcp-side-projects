import pandas as pd

# Try Windows encoding
df = pd.read_csv("sample_sales_data.csv", encoding="cp1252")

# Or, if that fails, ISO-8859-1
# df = pd.read_csv("sample_sales_data.csv", encoding="ISO-8859-1")

country_column = "COUNTRY"  # adjust to your CSV

for country in df[country_column].dropna().unique():
    country_df = df[df[country_column] == country]
    safe_country = country.replace(" ", "_").replace("/", "_")
    country_df.to_csv(f"{safe_country}.csv", index=False)

print("Files created per country successfully!")