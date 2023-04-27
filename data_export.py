import pandas as pd
import zipfile
import json
import io
from IPython.display import display
from datetime import datetime

zip_file_path = 'datatest (1).zip'
data = {}
output_file = "utills/output_data.json"



def generate_schema(zip_path, output_file=None):
    schema = {}

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        for zip_info in zip_file.infolist():
            if zip_info.filename.endswith(".json"):
                with zip_file.open(zip_info) as json_file:
                    data = json.load(io.TextIOWrapper(json_file, encoding="utf-8"))
                for key in data.keys():
                    if key not in schema:
                        schema[key] = {}
                    if isinstance(data[key], dict):
                        for subkey in data[key].keys():
                            if subkey not in schema[key]:
                                schema[key][subkey] = type(data[key][subkey]).__name__
                    elif isinstance(data[key], list):
                        for element in data[key]:
                            if isinstance(element, dict):
                                for subkey in element.keys():
                                    if subkey not in schema[key]:
                                        schema[key][subkey] = type(element[subkey]).__name__

    schema = {k: {sk: v for sk, v in sorted(sv.items())} for k, sv in schema.items()}
    schema = dict(sorted(schema.items()))

    return schema
    #exports file
    # with open(output_file, "w") as output:
    #     json.dump(schema, output, indent=4)

def retrun_header(key, data):
    return list(data[key].keys())

def data_divider_generator(zip_file_path,output_file=None) :
    schema = {}

    with zipfile.ZipFile(zip_file_path, "r") as zip_file:
        for zip_info in zip_file.infolist():
            if zip_info.filename.endswith(".json"):
                with zip_file.open(zip_info) as json_file:
                    data = json.load(io.TextIOWrapper(json_file, encoding="utf-8"))
                for key in data.keys():
                    if key not in schema:
                        schema[key] = []
                    if isinstance(data[key], dict):
                        schema[key].append(data[key])
                    elif isinstance(data[key], list):
                        for element in data[key]:
                            if isinstance(element, dict):
                                schema[key].append(element)

    
    schema = {k: sorted(v, key=lambda x: list(x.keys())) for k, v in schema.items()}
    schema = dict(sorted(schema.items()))
    return schema
    #exports file
    # with open(output_file, "w") as output:
    #     json.dump(schema, output, indent=4)

base_schema = generate_schema(zip_file_path)
assortment = retrun_header("assortment", base_schema)
marketplace = retrun_header("marketplace", base_schema)

schema = data_divider_generator(zip_file_path)

assortment_df = pd.DataFrame.from_dict(schema["assortment"])
marketplace_df = pd.DataFrame.from_dict(schema["marketplace"])


merged_AR =pd.merge(assortment_df, marketplace_df, on='idRetailerSKU', how='outer', suffixes=('_assortment_df', '_marketplace_df'))


filters = ['retailerPrice', 'manufacturerPrice', 'priceVariation', 'available'] 

filtered_AR =  merged_AR.loc[merged_AR['seller_marketplace_df'] == 'Americanas']


variation_dataset = filtered_AR[['retailerProductCode','manufacturerTitle','idRetailerSKU','manufacturerPrice', 'retailerPrice','retailerFromPrice','seller_marketplace_df', 'variation', 'available']]
variation_dataset_filtered = variation_dataset.loc[variation_dataset['variation'].notna()]

top_10_price_variations = variation_dataset_filtered.groupby('manufacturerTitle').apply(lambda x: x.sort_values(by='variation', ascending=False).head(1))
top_10_price_variations['price_diff'] = top_10_price_variations['retailerPrice'] - top_10_price_variations['manufacturerPrice']
top_10_price_variations = top_10_price_variations[['manufacturerTitle', 'idRetailerSKU', 'variation', 'price_diff']].sort_values(by='variation', ascending=False).head(10)
top_10_price_variations.to_csv(f'exported_data/top_10_price_variations_positive_{datetime.today().strftime("%Y_%m_%d")}.csv', index = True)


top_10_price_variations = variation_dataset_filtered.groupby('manufacturerTitle').apply(lambda x: x.sort_values(by='variation', ascending=True).head(1))
top_10_price_variations['price_diff'] = top_10_price_variations['retailerPrice'] - top_10_price_variations['manufacturerPrice']
top_10_price_variations = top_10_price_variations[['manufacturerTitle', 'idRetailerSKU', 'variation', 'price_diff']].sort_values(by='variation', ascending=True).head(10)

top_10_price_variations.to_csv(f'exported_data/top_10_price_variations_negative_{datetime.today().strftime("%Y_%m_%d")}.csv', index = True)


availability_df = filtered_AR[['retailerProductCode','manufacturerTitle','idRetailerSKU','manufacturerPrice', 'retailerPrice','retailerFromPrice','seller_marketplace_df', 'variation', 'available']]
availability_df_filtered = availability_df.loc[variation_dataset['available'].notna()]



top_10_unavailable = availability_df_filtered.sort_values(by='available', ascending=False).groupby('manufacturerTitle').head(1).sort_values(by='available')[:10]

top_10_unavailable['num_false'] = availability_df_filtered.groupby('manufacturerTitle')['available'].transform(lambda x: x.value_counts().get(False, 0))
top_10_unavailable = top_10_unavailable.sort_values(by='num_false', ascending=False)

top_10_unavailable.to_csv(f'exported_data/10_unavailable_{datetime.today().strftime("%Y_%m_%d")}.csv', index = True)


avg_retailer_price = variation_dataset_filtered.groupby('manufacturerTitle')['retailerPrice'].mean().reset_index()
avg_retailer_price.columns = ['manufacturerTitle', 'avg_retailer_price']

avg_manufacturer_price = variation_dataset_filtered.groupby('manufacturerTitle')['manufacturerPrice'].mean().reset_index()
avg_manufacturer_price.columns = ['manufacturerTitle', 'avg_manufacturer_price']

price_variation = variation_dataset_filtered.groupby('manufacturerTitle').apply(lambda x: (x['retailerPrice'] - x['manufacturerPrice']).mean()).reset_index()
price_variation.columns = ['manufacturerTitle', 'price_variation']

availability_rate = variation_dataset_filtered.groupby('manufacturerTitle')['available'].mean().reset_index()
availability_rate.columns = ['manufacturerTitle', 'availability_rate']

price_variation_rate = variation_dataset_filtered.groupby('manufacturerTitle').apply(lambda x: ((x['retailerPrice'] - x['manufacturerPrice']).abs() / x['manufacturerPrice']).mean()).reset_index()
price_variation_rate.columns = ['manufacturerTitle', 'price_variation_rate']

price_difference = variation_dataset_filtered.groupby('manufacturerTitle').apply(lambda x: (x['retailerPrice'] - x['manufacturerPrice']).mean()).reset_index()
price_difference.columns = ['manufacturerTitle', 'price_difference']

max_price = variation_dataset_filtered.groupby('manufacturerTitle')['retailerPrice'].max().reset_index()
max_price.columns = ['manufacturerTitle', 'max_price']
min_price = variation_dataset_filtered.groupby('manufacturerTitle')['retailerPrice'].min().reset_index()
min_price.columns = ['manufacturerTitle', 'min_price']

num_unavailable = variation_dataset_filtered.groupby('manufacturerTitle')['available'].apply(lambda x: sum(x==False)).reset_index()
num_unavailable.columns = ['manufacturerTitle', 'num_unavailable']

metrics_df = avg_retailer_price.merge(avg_manufacturer_price, on='manufacturerTitle')
metrics_df = metrics_df.merge(price_variation, on='manufacturerTitle')
metrics_df = metrics_df.merge(availability_rate, on='manufacturerTitle')
metrics_df = metrics_df.merge(price_variation_rate, on='manufacturerTitle')
metrics_df = metrics_df.merge(price_difference, on='manufacturerTitle')
metrics_df = metrics_df.merge(max_price, on='manufacturerTitle')
metrics_df = metrics_df.merge(min_price, on='manufacturerTitle')
metrics_df = metrics_df.merge(num_unavailable, on='manufacturerTitle')

metrics_df.to_csv(f'exported_data/metrics_{datetime.today().strftime("%Y_%m_%d")}.csv', index = True)




