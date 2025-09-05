import pandas as pd

data = pd.read_csv('sales_data_5m.csv')
print(data.columns)
print(data.shape)

dat = data[['Price','Quantity']]
#print(dat.head)

dat["Product"] = dat["Price"] * dat["Quantity"]
product_list = dat["Product"].tolist()

print(sum(product_list))