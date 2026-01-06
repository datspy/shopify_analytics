from operator import itemgetter
import pandas as pd
import functools
from access_functions import write_dataframe_to_bigquery
import os
from dotenv import load_dotenv
import logging


load_dotenv()

p_id = os.getenv("GCP_PROJECT_ID")
d_id = os.getenv("BIGQUERY_DATASET_ID")
logger = logging.getLogger(__name__)


def get_sales_df(table_data):

        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        product_variant_title = list(map(itemgetter('product_variant_title'), table_data['rows']))
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        orders = list(map(itemgetter('orders'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))
        average_order_value = list(map(itemgetter('average_order_value'), table_data['rows']))

        sales_dict = {'product_title': product_title,
                    'Variant': product_variant_title,
                    'SKU': product_variant_sku,
                    'Orders': orders,
                    'net_sales': net_sales,
                    'AOV': average_order_value}
        sales_df = pd.DataFrame(sales_dict)

        sales_df['Orders']=sales_df['Orders'].astype(int)
        sales_df['net_sales']=sales_df['net_sales'].astype(float)
        sales_df['AOV']=sales_df['AOV'].astype(float)

        sales_df = sales_df.reset_index(drop=True)

        return sales_df


def get_inventory_df(table_data):

        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

        inventory_dict = {'SKU': product_variant_sku,
                        'Inventory Sold': inventory_units_sold,
                        'Ending Units': ending_inventory_units}
        inventory_df = pd.DataFrame(inventory_dict)

        inventory_df['Inventory Sold']=inventory_df['Inventory Sold'].astype(int)
        inventory_df['Ending Units']=inventory_df['Ending Units'].astype(int)

        inventory_df = inventory_df.rename(columns={'Inventory Sold':'inventory_sold_last_14days', 'Ending Units': 'current_available_inventory_units'}).reset_index(drop=True)

        return inventory_df


def get_inventory_weekly_df(table_data):

        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        week = list(map(itemgetter('week'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

        inventory_weekly_dict = {'SKU': product_variant_sku,
                        'Week': week,
                        'Inventory Sold': inventory_units_sold,
                        'Ending Units': ending_inventory_units}
        inventory_weekly_df = pd.DataFrame(inventory_weekly_dict)

        inventory_weekly_df['Week']=pd.to_datetime(inventory_weekly_df['Week']).dt.date
        inventory_weekly_df['Inventory Sold']=inventory_weekly_df['Inventory Sold'].astype(int)
        inventory_weekly_df['Ending Units']=inventory_weekly_df['Ending Units'].astype(int)        
        inventory_weekly_df['Active_Weeks'] = inventory_weekly_df.apply(lambda x : 1 if x['Inventory Sold']>0 else 0, axis=1)
        inventory_weekly_df['Inactive_Weeks'] = inventory_weekly_df.apply(lambda x : 1 if ((x['Ending Units']>0) & (x['Inventory Sold']==0)) else 0, axis=1)
        inventory_weekly_df['OutOfStock_Weeks'] = inventory_weekly_df.apply(lambda x : 1 if (x['Ending Units']==0) else 0, axis=1)

        inventory_agg = inventory_weekly_df.groupby(['SKU'], as_index=False).agg({'Week':'count', 'Inventory Sold': sum, 'Active_Weeks': sum, 'Inactive_Weeks': sum, 'OutOfStock_Weeks': sum })
        active_weeks = inventory_agg['Active_Weeks'].replace(0, pd.NA)
        inventory_agg['avg_weekly_sales'] = (inventory_agg['Inventory Sold'] / active_weeks).fillna(0)


        inventory_agg_data = inventory_agg[['SKU', 'Inventory Sold', 'OutOfStock_Weeks', 'avg_weekly_sales']].rename(columns={'Inventory Sold':'inventory_sold_last_6months', 
                                                                                                                                'OutOfStock_Weeks': 'out_of_stock_weeks_last_6months'
                                                                                                                                }).reset_index(drop=True)

        return inventory_agg_data


def get_sales_by_channel_df(table_data):        
       
        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        sales_channel = list(map(itemgetter('sales_channel'), table_data['rows']))
        orders = list(map(itemgetter('orders'), table_data['rows']))
        quantity_returned = list(map(itemgetter('quantity_returned'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))
        average_order_value = list(map(itemgetter('average_order_value'), table_data['rows']))

        channel_sales_dict = {'Product_Title': product_title,
                'sales_channel': sales_channel,
                'orders': orders,
                'quantity_returned': quantity_returned,               
                'net_sales': net_sales,
                'AOV': average_order_value}
        channel_sales_df = pd.DataFrame(channel_sales_dict)

        channel_sales_df['orders']=channel_sales_df['orders'].astype(int)
        channel_sales_df['quantity_returned']=channel_sales_df['quantity_returned'].astype(int)
        channel_sales_df['net_sales']=channel_sales_df['net_sales'].astype(float)
        channel_sales_df['AOV']=channel_sales_df['AOV'].astype(float)

        channel_sales_df = channel_sales_df.reset_index(drop=True)

        return channel_sales_df


def get_inventory_for_channel_products_df(table_data):        
       
        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        product_variant_title = list(map(itemgetter('product_variant_title'), table_data['rows']))
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))
        days_out_of_stock = list(map(itemgetter('days_out_of_stock'), table_data['rows']))
        sell_through_rate = list(map(itemgetter('sell_through_rate'), table_data['rows']))

        inventory_channel_dict = {'Product_Title': product_title,
                                'Product_Variant_Title': product_variant_title,
                                'SKU': product_variant_sku,
                                'inventory_units_sold': inventory_units_sold,
                                'ending_inventory_units': ending_inventory_units,
                                'days_out_of_stock': days_out_of_stock,
                                'sell_through_rate': sell_through_rate
                                }
        inventory_channel_df = pd.DataFrame(inventory_channel_dict)

        inventory_channel_df['inventory_units_sold']=inventory_channel_df['inventory_units_sold'].astype(int)
        inventory_channel_df['ending_inventory_units']=inventory_channel_df['ending_inventory_units'].astype(int)        
        inventory_channel_df['days_out_of_stock']=inventory_channel_df['days_out_of_stock'].astype(int)        
        inventory_channel_df['sell_through_rate']=inventory_channel_df['sell_through_rate'].astype(float)
        inventory_channel_df['OutOfStock_SKU'] = inventory_channel_df.apply(lambda x : 1 if (x['ending_inventory_units']==0) else 0, axis=1) 

        inventory_channel_df = inventory_channel_df.reset_index(drop=True)
        return inventory_channel_df


def get_consolidated_df(sales_df, inventory_df, inventory_weekly_df):

    consolidated_df = functools.reduce(lambda left, right: pd.merge(left, right, on='SKU'), [sales_df, inventory_df, inventory_weekly_df])
    consolidated_df = consolidated_df.fillna(0)

    return consolidated_df


def load_bigquery_table(final_df, table_name):
        logger.info("Loading table %s to BigQuery", table_name)
        write_dataframe_to_bigquery(               
               df=final_df,
               project_id=p_id,
               dataset_id=d_id,
               table_id=table_name,
               if_exists='replace'
               )
