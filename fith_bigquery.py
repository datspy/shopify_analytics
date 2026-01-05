import requests
import os
from dotenv import load_dotenv
import shopify
from access_functions import get_access_token_oauth, connect_to_shopify, run_shopifyQL_query
from core_functions import get_sales_df, get_inventory_df, get_inventory_weekly_df, get_consolidated_df, load_bigquery_table, get_sales_by_channel_df 
from core_functions import get_inventory_for_channel_products_df
from queries import get_sales_query, get_inventory_query, get_inventory_weekly_query, get_channel_sales_query, get_channel_inventory_query

load_dotenv()

SHOP_URL = os.getenv("SHOP_URL")


def main(access_token=None):
    """Main function to pull and analyze inventory data"""
    try:
        # Connect to Shopify
        connect_to_shopify(access_token)
        sales_query = get_sales_query()
        sales_data = run_shopifyQL_query(sales_query, access_token)
        sales_df = get_sales_df(sales_data)
        skus = tuple(sales_df['SKU'].unique())
        print(sales_df.shape)

        inventory_query = get_inventory_query(skus)
        inventory_data = run_shopifyQL_query(inventory_query, access_token)
        inventory_df = get_inventory_df(inventory_data)
        print(inventory_df.shape)

        inventory_weekly_query = get_inventory_weekly_query(skus)
        inventory_weekly_data = run_shopifyQL_query(inventory_weekly_query, access_token)
        inventory_weekly_df = get_inventory_weekly_df(inventory_weekly_data)
        print(inventory_weekly_df.shape)


        sales_by_channel_query = get_channel_sales_query()
        sales_by_channel_data = run_shopifyQL_query(sales_by_channel_query, access_token)
        channel_sales_df = get_sales_by_channel_df(sales_by_channel_data)
        products= tuple(channel_sales_df['Product_Title'].unique())

        channel_inventory_df = get_channel_inventory_query(products)
        inventory_by_channel_data = run_shopifyQL_query(channel_inventory_df, access_token)
        channel_inventory_df = get_inventory_for_channel_products_df(inventory_by_channel_data)
        
        out_of_stock_df = channel_inventory_df[channel_inventory_df['OutOfStock_SKU']==1].reset_index(drop=True)
        out_of_stock_df = out_of_stock_df.fillna(0)
        
        channel_inventory_agg = channel_inventory_df.groupby(['Product_Title']).agg({'SKU':'count', 'OutOfStock_SKU': sum, 'inventory_units_sold': 'sum'})
        channel_consolidated_df = channel_sales_df.merge(channel_inventory_agg, how='left', on='Product_Title')

        # Merge DataFrames
        final_df = get_consolidated_df(sales_df, inventory_df, inventory_weekly_df)        
        
        return final_df, channel_consolidated_df, out_of_stock_df 

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clear the session
        shopify.ShopifyResource.clear_session()

if __name__ == "__main__":
    # Step 1: Get Access Token
    data, access_token = get_access_token_oauth(SHOP_URL)
    
    # Step 2: Call Main Function
    df, channel_df, out_of_stock_df = main(access_token)

    # Step 3: Load Data to BigQuery
    load_bigquery_table(df, 'top_sku_data')
    load_bigquery_table(channel_df, 'channel_sales_data')
    load_bigquery_table(out_of_stock_df, 'out_of_stock_data')
    # channel_df.to_csv('channel_sales_data.csv', index=False)