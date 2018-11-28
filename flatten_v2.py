import pandas as pd 
import numpy as np 
import json 
from ast import literal_eval


def flatten(in_csv, out_csv, nrows=None):
    df = pd.read_csv(in_csv, dtype=np.object, nrows=nrows)
    # json columns
    json_cols = ['device', 'geoNetwork', 'totals', 'trafficSource']

    def parse_json_col(raw_str):
        return pd.Series(json.loads(raw_str))
    
    for col in json_cols:
        parsed_df = df[col].apply(parse_json_col)
        parsed_df.columns = [f'{col}_{x}' for x in parsed_df.columns]
        df = pd.concat([df, parsed_df], axis=1)
        df.drop(col, axis=1, inplace=True)
    
    # trafficSource_adwordsClickInfo
    trafficSource_adwordsClickInfo_df = df.trafficSource_adwordsClickInfo.apply(pd.Series)
    trafficSource_adwordsClickInfo_df.columns = [f'trafficSource_adwordsClickInfo_{x}' for x in trafficSource_adwordsClickInfo_df.columns]
    df = pd.concat([df, trafficSource_adwordsClickInfo_df], axis=1)
    df.drop('trafficSource_adwordsClickInfo', axis=1, inplace=True)

    # customDimensions
    def parse_customDimensions(raw_str):
        lst = literal_eval(raw_str)
        if isinstance(lst, list) and lst:
            return pd.Series(lst[0])
        else:
            return pd.Series({})
    
    customDimensions_df = df.customDimensions.apply(parse_customDimensions)
    customDimensions_df.columns = [f'customDimensions_{x}' for x in customDimensions_df.columns]
    df = pd.concat([df, customDimensions_df], axis=1)
    df.drop('customDimensions', axis=1, inplace=True)

    # hits
    def parse_hits(raw_str):
        lst = literal_eval(raw_str)
        if isinstance(lst, list) and lst:
            return pd.Series(lst[0])
        else:
            return pd.Series({})
    
    hits_df = df.hits.apply(parse_hits)
    hits_df.columns = [f'hits_{x}' for x in hits_df.columns]
    df = pd.concat([df, hits_df], axis=1)
    df.drop('hits', axis=1, inplace=True)

    dict_cols = ['hits_page', 'hits_transaction', 'hits_item', 'hits_appInfo', 
        'hits_exceptionInfo', 'hits_eCommerceAction', 'hits_social', 'hits_contentGroup', 'hits_promotionActionInfo']
    for col in dict_cols:
        parsed_df = hits_df[col].apply(pd.Series)
        parsed_df.columns = [f'{col}_{x}' for x in parsed_df.columns]
        df = pd.concat([df, parsed_df], axis=1)
        df.drop(col, axis=1, inplace=True)
    
    # 'hits_experiment', 'hits_customVariables', 'hits_customMetrics', 'hits_publisher_infos', 'hits_customDimensions' are empty
    df.drop(['hits_experiment', 'hits_customVariables', 'hits_customMetrics', 'hits_publisher_infos', 'hits_customDimensions'], axis=1, inplace=True)

    # 'hits_product', 'hits_promotion'
    def parse_list(x):
        if isinstance(x, list) and x:
            return pd.Series(x[0])
        else:
            return pd.Series({})
    
    for col in ['hits_product', 'hits_promotion']:
        parsed_df = hits_df[col].apply(parse_list)
        parsed_df.columns = [f'{col}_{x}' for x in parsed_df.columns]
        df = pd.concat([df, parsed_df], axis=1)
        df.drop(col, axis=1, inplace=True)

    df.to_csv(out_csv, index=False)

    return df.shape


if __name__ == "__main__":
    flatten("test_v2.csv", "test_v2_flatten.csv")
    flatten("train_v2.csv", "train_v2_flatten.csv")
