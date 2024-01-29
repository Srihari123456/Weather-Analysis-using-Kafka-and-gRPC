import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
import os
import json

month_series = defaultdict(list)
data_directory = "./files/"
# Iterate through partition files
for filename in os.listdir(data_directory):
    if filename.startswith("partition-") and filename.endswith(".json"):
        with open(os.path.join(data_directory, filename), 'r') as file:
            partition_info = json.load(file)
            #print("Printing partition_info.items()")
            #print(partition_info.items())
            for month, year_data in partition_info.items():
                if month not in ['partition','offset']:
                    print("Month = ",month)
                    #print("Year Data = ", year_data)
                    latest_year = max(year_data.keys()) if year_data else None
                    print("Latest year = ",latest_year)
                    if latest_year:
                        avg_temp = year_data[latest_year]['avg']
                        month_year = f"{month}-{latest_year}"
                        month_series[month_year].append(avg_temp)
                    
print("Month Series = ",month_series)
avg_monthly_temps = {}
for month_year, temps in month_series.items():
    avg_monthly_temps[month_year] = sum(temps) / len(temps)

# Create Pandas Series
month_series = pd.Series(avg_monthly_temps)
#print(month_series)
print("Month Series Pandas = ",month_series)

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("./files/month.svg")

