import regex as re
import json
import pandas as pd
from config.logger import Log4j


def fix_json():
    with open('../input/data_source_3.json', "r", encoding="utf-8") as f:
        bad_json = f.read()

    fix_1 = re.sub(r"(\"\w+\") (\"\w+\s\w+\s\d),", r'\1: \2",', bad_json)
    fix_2 = re.sub(r"(})(\n\s*\"\w+\": \"\d+\",)", r'\1,\n{\2', fix_1)
    fix_3 = re.sub(r"device_model", "first_device_type", fix_2)

    json_list = json.loads(fix_3)
    df = pd.DataFrame(json_list)

    df.to_parquet('../output/fixed_json.parquet')
  #  return df
  #  print(df2.head(10))


def read_parquet():
    df2 = pd.read_parquet('../output/fixed_json.parquet')

    return df2


def aggregate_model(df):

    df = pd.concat(i for _, i in df.groupby("id") if len(i) > 1)
    df[["model_name"]] = [df['first_device_type'].str.split(" ", n=0, expand=True)]

    df_model = pd.concat(m for _, m in df.groupby(["id", "model_name"]) if len(m) > 1)
    
    df_device = (df_model.set_index(['id', df_model.groupby('id').cumcount()])['first_device_type']
                 .unstack(fill_value='').rename(columns = lambda x : f'device_model_{x+1}'))

    df_created = (df_model.set_index(['id', df_model.groupby('id').cumcount()])['created_at']
                  .unstack(fill_value='').rename(columns = lambda y : f'created_at_{y+1}'))

    df_result = pd.concat([df_device, df_created], axis=1).reset_index()
    print(df_result)
    df_result.to_parquet('../output/aggregated_model.parquet')
    df_result.to_csv('../output/aggregated_model.csv')
    return df_result
