import os
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv
import shopify
from access_functions import get_access_token_oauth, connect_to_shopify, run_shopifyQL_query, read_dataframe_from_bigquery
import core_functions as core
import queries as qry
import time

load_dotenv()

SHOP_URL = os.getenv("SHOP_URL")
logger = logging.getLogger(__name__)


def setup_logging(log_path="logs/run.log"):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ],
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", choices=["bigquery", "csv"], default="bigquery")
    parser.add_argument("--csv-dir", default="output")
    return parser.parse_args()


def main(access_token=None):
    """Main function to pull and analyze inventory data"""
    try:
        # Connect to Shopify
        connect_to_shopify(access_token)
        logger.info("Running sales query")
        sales_query = qry.get_all_sku_sales_query()
        sales_data = run_shopifyQL_query(sales_query, access_token)
        sales_df = core.get_sales_df(sales_data)
        skus = sales_df['product_variant_sku'].unique()
        skus.sort()
        skus_sorted = tuple(skus)
        logger.info("Sales rows: %s", sales_df.shape)


        logger.info("Running top-selling query")
        top_seller_query = qry.get_top_selling_query()
        top_seller_data = run_shopifyQL_query(top_seller_query, access_token)
        top_seller_df = core.get_sales_df(top_seller_data)
        top_seller_df = top_seller_df[['product_variant_sku', 'net_sales']].rename(columns={'net_sales': 'net_sales_14days'}).reset_index(drop=True)

        logger.info("Running inventory query")
        inventory_query = qry.get_inventory_query(skus_sorted)
        inventory_data = run_shopifyQL_query(inventory_query, access_token)
        inventory_df = core.get_inventory_df(inventory_data)
        logger.info("Inventory rows: %s", inventory_df.shape)

        logger.info("Running inventory weekly query")
        inventory_weekly_agg_query = qry.get_inventory_agg_query()
        inventory_weekly_agg_data = read_dataframe_from_bigquery(inventory_weekly_agg_query)
        inventory_weekly_agg_df = inventory_weekly_agg_data[['product_variant_sku','active_weeks', 'out_of_stock_weeks', 'avg_weekly_sales']].reset_index(drop=True)        
        logger.info("Inventory weekly rows: %s", inventory_weekly_agg_df.shape)

        logger.info("Running all-sku channel sales query")
        all_sku_channel_sales_query = qry.get_all_sku_channel_sales_query()
        all_sku_channel_sales_data = run_shopifyQL_query(all_sku_channel_sales_query, access_token)
        all_sku_channel_sales_df = core.get_sku_channel_sales_df(all_sku_channel_sales_data)
        logger.info("All SKU channel sales rows: %s", all_sku_channel_sales_df.shape)

        logger.info("Running channel sales query")
        sales_by_channel_query = qry.get_channel_sales_query()
        sales_by_channel_data = run_shopifyQL_query(sales_by_channel_query, access_token)
        channel_sales_df = core.get_sales_by_channel_df(sales_by_channel_data)
        products= tuple(channel_sales_df['product_title'].unique())

        logger.info("Sleeping for 10 seconds to avoid rate limits...")
        time.sleep(600)  # Sleep to avoid hitting rate limits

        logger.info("Running channel inventory query")
        channel_inventory_query = qry.get_channel_inventory_query(products)
        inventory_by_channel_data = run_shopifyQL_query(channel_inventory_query, access_token)
        channel_inventory_df = core.get_inventory_for_channel_products_df(inventory_by_channel_data)
        
        out_of_stock_df = channel_inventory_df[channel_inventory_df['out_of_stock_sku']==1].reset_index(drop=True)
        out_of_stock_df = out_of_stock_df.fillna(0)
        
        channel_inventory_agg = channel_inventory_df.groupby(['product_title'], as_index=False).agg({'product_variant_sku':'count',
                                                                                     'out_of_stock_sku': sum,
                                                                                     'inventory_units_sold': sum}).rename(columns={'product_variant_sku':'active_sku_count'}).reset_index(drop=True)
        
        channel_consolidated_df = channel_sales_df.merge(channel_inventory_agg, how='left', on='product_title')
        channel_consolidated_df = channel_consolidated_df[['product_title', 'sales_channel', 'inventory_units_sold', 
                                                       'orders', 'quantity_returned', 'net_sales', 'average_order_value', 
                                                       'active_sku_count', 'out_of_stock_sku']]

        # Merge DataFrames
        df_list = [sales_df, top_seller_df, inventory_df, inventory_weekly_agg_df, all_sku_channel_sales_df]
        final_df = core.get_consolidated_df(df_list)
        
        return final_df, channel_consolidated_df, out_of_stock_df 

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
    df, channel_df, out_of_stock_df = main(access_token)    

    # Step 3: Load Data to BigQuery
    if args.output == "bigquery":
        core.load_bigquery_table(df, 'top_sku_data')
        core.load_bigquery_table(channel_df, 'channel_sales_data')
        core.load_bigquery_table(out_of_stock_df, 'out_of_stock_data')
    else:
        os.makedirs(args.csv_dir, exist_ok=True)
        df.to_csv(os.path.join(args.csv_dir, "top_sku_data.csv"), index=False)
        channel_df.to_csv(os.path.join(args.csv_dir, "channel_sales_data.csv"), index=False)
        out_of_stock_df.to_csv(os.path.join(args.csv_dir, "out_of_stock_data.csv"), index=False)
