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
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv()

SHOP_URL = os.getenv("SHOP_URL")
logger = logging.getLogger(__name__)

## Six-Month window dates
today = datetime.today().date()

## Splitting Data into Three 4-Month Periods
first_st_date = (today - relativedelta(months=12)).replace(day=1)
first_end_date = (first_st_date + relativedelta(months=4)).replace(day=1) - relativedelta(days=1)
second_st_date = first_end_date + relativedelta(days=1)
second_end_date = (second_st_date + relativedelta(months=4)).replace(day=1) - relativedelta(days=1)
third_st_date = second_end_date + relativedelta(days=1)
third_end_date = (third_st_date + relativedelta(months=4)).replace(day=1) - relativedelta(days=1)

# end_date = today.replace(day=1) - relativedelta(days=1)

dates_list = [(first_st_date, first_end_date), (second_st_date, second_end_date), (third_st_date, third_end_date)]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", choices=["bigquery", "csv"], default="bigquery")
    parser.add_argument("--csv-dir", default="output")
    return parser.parse_args()


def cross_join_date_table(df):
    """
    Cross Join Date Table with Sales Data
    """
    date_table = core.create_date_table(2025)
    date_table['month']=pd.to_datetime(date_table['year_month'].dt.to_timestamp()).dt.date
    df_months = date_table[['month']].drop_duplicates().reset_index(drop=True)
    df_unique = df[['product_title','product_variant','product_variant_sku']].drop_duplicates().reset_index(drop=True)
    product_months = df_unique.merge(df_months, how='cross')
    df_final = df.merge(product_months, on=['product_title','product_variant','product_variant_sku', 'month'], how='outer')
    df_final = df_final[['product_title', 'product_variant', 'product_variant_sku', 'month', 'net_items_sold', 'orders',
                         'quantity_returned', 'net_sales', 'gross_sales', 'discounts', 'net_returns']].fillna(0)
    df_final = df_final.sort_values(by=['product_title','product_variant_sku','month']).reset_index(drop=True)    

    return df_final


def get_product_sales_query(start_date, end_date):
    """
    Get Query for Top-10 SKU by sales in last 14 days.
    """
    final_query = fr"""
        FROM sales
        SHOW net_items_sold, orders, net_sales, gross_sales, discounts, returns,
            average_order_value, quantity_returned
        WHERE line_type = 'product'
            AND product_variant_sku IS NOT NULL
        GROUP BY month, product_title, product_variant_title, product_variant_sku
        HAVING net_items_sold > 0
        SINCE {start_date} UNTIL {end_date}
        ORDER BY product_title, product_variant_sku        
        """
    
    return final_query


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


def transform_yearly_data(table_data):        
       
        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        product_variant = list(map(itemgetter('product_variant_title'), table_data['rows']))
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        net_items_sold = list(map(itemgetter('net_items_sold'), table_data['rows']))
        gross_sales = list(map(itemgetter('gross_sales'), table_data['rows']))
        discounts = list(map(itemgetter('discounts'), table_data['rows']))
        returns = list(map(itemgetter('returns'), table_data['rows']))
        orders = list(map(itemgetter('orders'), table_data['rows']))
        quantity_returned = list(map(itemgetter('quantity_returned'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))
        average_order_value = list(map(itemgetter('average_order_value'), table_data['rows']))
        month = list(map(itemgetter('month'), table_data['rows']))        

        transformed_dict = {'product_title': product_title,
                'product_variant': product_variant,
                'product_variant_sku': product_variant_sku,
                'month': month,
                'net_items_sold': net_items_sold,
                'gross_sales': gross_sales,
                'discounts': discounts,
                'net_returns': returns,
                'orders': orders,
                'quantity_returned': quantity_returned,               
                'net_sales': net_sales,
                'average_order_value': average_order_value}
        t_df = pd.DataFrame(transformed_dict)

        t_df['net_items_sold']=t_df['net_items_sold'].astype(int)
        t_df['orders']=t_df['orders'].astype(int)                    
        t_df['quantity_returned']=t_df['quantity_returned'].astype(int)    
        t_df['net_returns']=t_df['net_returns'].astype(float)    
        t_df['average_order_value']=t_df['average_order_value'].astype(float)
        t_df['net_sales']=t_df['net_sales'].astype(float)
        t_df['gross_sales']=t_df['gross_sales'].astype(float)
        t_df['discounts']=t_df['discounts'].astype(float)
        t_df['month']=pd.to_datetime(t_df['month']).dt.date


        t_df = t_df.reset_index(drop=True)

        return t_df


def main(access_token=None):
    """Main function to pull and analyze inventory data"""
    try:
        # Connect to Shopify
        connect_to_shopify(access_token)
        logger.info("Running yearly-data query")
        
        transformed_df = pd.DataFrame()

        for st_dt, end_dt in dates_list:

            product_sales_query = get_product_sales_query(st_dt, end_dt)
            year_sales_data = run_shopifyQL_query(product_sales_query, access_token)
            part_df = transform_yearly_data(year_sales_data)              
            transformed_df = pd.concat([transformed_df, part_df], ignore_index=True)

        transformed_df = transformed_df.reset_index(drop=True)

        full_df = cross_join_date_table(transformed_df)

        return full_df

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
        core.load_bigquery_table(df, 'all_sku_data')
        pass
    else:
        os.makedirs(args.csv_dir, exist_ok=True)
        df.to_csv(os.path.join(args.csv_dir, "all_sku_yearly_data.csv"), index=False)
