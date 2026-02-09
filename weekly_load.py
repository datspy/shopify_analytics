import os
import argparse
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import shopify
from operator import itemgetter
from access_functions import get_access_token_oauth, connect_to_shopify, run_shopifyQL_query
import core_functions as core
import queries as qry
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

load_dotenv()

SHOP_URL = os.getenv("SHOP_URL")
logger = logging.getLogger(__name__)

## Six-Month window dates
today = datetime.today().date()

## Splitting Data into Three 4-Month Periods
# start_date = (today - relativedelta(months=12)).replace(day=1)
# end_date = (first_st_date + relativedelta(months=4)).replace(day=1) - relativedelta(days=1)



def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", choices=["bigquery", "csv"], default="bigquery")
    parser.add_argument("--csv-dir", default="output")
    return parser.parse_args()


def setup_logging(log_path="logs/one_time_run.log"):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ],
    )


def get_inventory_sold_df(table_data):

        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        product_variant_title = list(map(itemgetter('product_variant_title'), table_data['rows']))
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

        inventory_dict = {'product_title': product_title,
                        'product_variant': product_variant_title,
                        'product_variant_sku': product_variant_sku,
                        'inventory_sold_last_14days': inventory_units_sold,
                        'current_available_inventory_units': ending_inventory_units}
        inventory_df = pd.DataFrame(inventory_dict)

        inventory_df['inventory_sold_last_14days']=inventory_df['inventory_sold_last_14days'].astype(int)
        inventory_df['current_available_inventory_units']=inventory_df['current_available_inventory_units'].astype(int)
        inventory_df = inventory_df.reset_index(drop=True)

        return inventory_df


def get_inventory_weekly_raw_df(table_data):

        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        week = list(map(itemgetter('week'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

        inventory_weekly_dict = {'product_variant_sku': product_variant_sku,
                        'week': week,
                        'inventory_units_sold': inventory_units_sold,
                        'ending_inventory_units': ending_inventory_units}
        inventory_weekly_df = pd.DataFrame(inventory_weekly_dict)

        inventory_weekly_df['week']=pd.to_datetime(inventory_weekly_df['week']).dt.date
        inventory_weekly_df['inventory_units_sold']=inventory_weekly_df['inventory_units_sold'].astype(int)
        inventory_weekly_df['ending_inventory_units']=inventory_weekly_df['ending_inventory_units'].astype(int)        
        inventory_weekly_df['active_weeks'] = inventory_weekly_df.apply(lambda x : 1 if x['inventory_units_sold']>0 else 0, axis=1)
        inventory_weekly_df['inactive_weeks'] = inventory_weekly_df.apply(lambda x : 1 if ((x['ending_inventory_units']>0) & (x['inventory_units_sold']==0)) else 0, axis=1)
        inventory_weekly_df['out_of_stock_weeks'] = inventory_weekly_df.apply(lambda x : 1 if (x['ending_inventory_units']<0) else 0, axis=1)

        inventory_weekly_df = inventory_weekly_df.reset_index(drop=True)

        return inventory_weekly_df


def main(access_token=None):
    """Main function to pull and analyze inventory data"""
    try:
        # Connect to Shopify
        connect_to_shopify(access_token)
        logger.info("Running yearly-data query")

        ### Get inventory data
        inventory_query = qry.get_all_sku_inventory_query()
        inventory_data = run_shopifyQL_query(inventory_query, access_token)
        inventory_sold_df = get_inventory_sold_df(inventory_data)

        inventory_sold_df_merge = inventory_sold_df[['product_title','product_variant','product_variant_sku']].drop_duplicates().reset_index(drop=True)

        skus = inventory_sold_df['product_variant_sku'].unique()
        skus.sort()
        skus_sorted = tuple(skus)

        ##### Get inventory data

        final_df = pd.DataFrame()

        for i in range(0, len(skus_sorted), 18):
            batch = skus_sorted[i:i+18]
            inventory_weekly_query = qry.get_all_sku_weekly_inventory_query(batch)
            inventory_weekly_data = run_shopifyQL_query(inventory_weekly_query, access_token)            
            print("sleeping for 10 secs")
            time.sleep(10)    
            inventory_weekly_raw_df = get_inventory_weekly_raw_df(inventory_weekly_data)
            final_df = pd.concat([final_df, inventory_weekly_raw_df], ignore_index=True)
            print(final_df.shape, inventory_weekly_raw_df.shape)      
        
        final_df = final_df.merge(inventory_sold_df_merge, how='left', on='product_variant_sku')
        final_df = final_df[['product_title', 'product_variant', 'product_variant_sku', 'week', 'inventory_units_sold',
                            'ending_inventory_units', 'active_weeks', 'inactive_weeks', 'out_of_stock_weeks']].reset_index(drop=True)
        return final_df

    except Exception as e:
        logger.exception("Error in main: %s", str(e))
        raise
    finally:
        # Clear the session
        shopify.ShopifyResource.clear_session()


if __name__ == "__main__":


    setup_logging()

    args = parse_args()
    # Step 1: Get Access Token
    access_token = get_access_token_oauth(SHOP_URL)
    
    # Step 2: Call Main Function
    df = main(access_token)

    '''
    NOTE: 
    To run the script, use one of the following commands:
    python fith_bigquery.py   ## Default is to load data to BigQuery   
    python fith_bigquery.py --output csv   ## To save data as CSV files
    '''
    # Step 3: Load Data to BigQuery
    if args.output == "bigquery":
        core.load_bigquery_table(df, 'all_sku_weekly_inventory_data')
        pass
    else:
        os.makedirs(args.csv_dir, exist_ok=True)
        df.to_csv(os.path.join(args.csv_dir, "all_sku_weekly_inventory_data.csv"), index=False)
