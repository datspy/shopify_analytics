from operator import itemgetter
import pandas as pd
import functools
from access_functions import write_dataframe_to_bigquery
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()

p_id = os.getenv("GCP_PROJECT_ID")
d_id = os.getenv("BIGQUERY_DATASET_ID")
logger = logging.getLogger(__name__)


def create_date_table(year):
    """
    Creates a date table for an entire year with various date dimensions.
    
    Parameters:
    year (int): The year for which to create the date table
    
    Returns:
    pd.DataFrame: A DataFrame containing date dimension columns
    """
    # Create date range for the entire year
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create the date table
    date_table = pd.DataFrame({
        'date': date_range,
        'year_month': date_range.to_period('M'),
        'quarter': date_range.to_series().apply(lambda x: f"Q{(x.month - 1) // 3 + 1}_{x.year}").values,        
        'week_number': date_range.isocalendar().week
    })
    
    date_table = date_table.reset_index(drop=True)

    return date_table


def get_sales_df(table_data):

        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        product_variant_title = list(map(itemgetter('product_variant_title'), table_data['rows']))
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        orders = list(map(itemgetter('orders'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))
        average_order_value = list(map(itemgetter('average_order_value'), table_data['rows']))

        sales_dict = {'product_title': product_title,
                    'product_variant': product_variant_title,
                    'product_variant_sku': product_variant_sku,
                    'orders': orders,
                    'net_sales': net_sales,
                    'average_order_value': average_order_value}
        sales_df = pd.DataFrame(sales_dict)

        sales_df['orders']=sales_df['orders'].astype(int)
        sales_df['net_sales']=sales_df['net_sales'].astype(float)
        sales_df['average_order_value']=sales_df['average_order_value'].astype(float)

        sales_df = sales_df.reset_index(drop=True)

        return sales_df


def get_inventory_df(table_data):

        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
        inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
        ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

        inventory_dict = {'product_variant_sku': product_variant_sku,
                        'inventory_sold_last_60days': inventory_units_sold,
                        'current_available_inventory_units': ending_inventory_units}
        inventory_df = pd.DataFrame(inventory_dict)

        inventory_df['inventory_sold_last_60days']=inventory_df['inventory_sold_last_60days'].astype(int)
        inventory_df['current_available_inventory_units']=inventory_df['current_available_inventory_units'].astype(int)
        inventory_df = inventory_df.reset_index(drop=True)

        return inventory_df



def get_inventory_weekly_agg_df(table_data):

        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))        
        inventory_units_sold = list(map(itemgetter('total_inventory_sold'), table_data['rows']))
        total_active_weeks = list(map(itemgetter('total_active_weeks'), table_data['rows']))
        total_out_of_stock_weeks = list(map(itemgetter('total_out_of_stock_weeks'), table_data['rows']))
        avg_weekly_sales = list(map(itemgetter('avg_weekly_sales'), table_data['rows']))



        inventory_weekly_agg_dict = {'product_variant_sku': product_variant_sku,                        
                        'inventory_units_sold': inventory_units_sold,
                        'active_weeks': total_active_weeks,
                        'out_of_stock_weeks': total_out_of_stock_weeks,
                        'avg_weekly_sales': avg_weekly_sales}
        
        inventory_weekly_agg_df = pd.DataFrame(inventory_weekly_agg_dict)
        inventory_agg_data = inventory_weekly_agg_df[['product_variant_sku','active_weeks', 'out_of_stock_weeks', 'avg_weekly_sales']].reset_index(drop=True)

        return inventory_agg_data


def get_sku_channel_sales_df(table_data):        
       
        product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))        
        orders = list(map(itemgetter('orders'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))

        sku_channel_sales_dict = {'product_variant_sku': product_variant_sku,                
                'tiktok_meta_orders': orders,               
                'tiktok_meta_net_sales': net_sales}
        sku_channel_sales_df = pd.DataFrame(sku_channel_sales_dict)

        sku_channel_sales_df['tiktok_meta_orders']=sku_channel_sales_df['tiktok_meta_orders'].astype(int)        
        sku_channel_sales_df['tiktok_meta_net_sales']=sku_channel_sales_df['tiktok_meta_net_sales'].astype(float)        
        sku_channel_sales_df = sku_channel_sales_df.reset_index(drop=True)

        return sku_channel_sales_df

# def get_inventory_weekly_df(table_data):

#         product_variant_sku = list(map(itemgetter('product_variant_sku'), table_data['rows']))
#         week = list(map(itemgetter('week'), table_data['rows']))
#         inventory_units_sold = list(map(itemgetter('inventory_units_sold'), table_data['rows']))
#         ending_inventory_units = list(map(itemgetter('ending_inventory_units'), table_data['rows']))

#         inventory_weekly_dict = {'product_variant_sku': product_variant_sku,
#                         'week': week,
#                         'inventory_units_sold': inventory_units_sold,
#                         'ending_inventory_units': ending_inventory_units}
#         inventory_weekly_df = pd.DataFrame(inventory_weekly_dict)

#         inventory_weekly_df['week']=pd.to_datetime(inventory_weekly_df['week']).dt.date
#         inventory_weekly_df['inventory_units_sold']=inventory_weekly_df['inventory_units_sold'].astype(int)
#         inventory_weekly_df['ending_inventory_units']=inventory_weekly_df['ending_inventory_units'].astype(int)        
#         inventory_weekly_df['Active_Weeks'] = inventory_weekly_df.apply(lambda x : 1 if x['inventory_units_sold']>0 else 0, axis=1)
#         inventory_weekly_df['Inactive_Weeks'] = inventory_weekly_df.apply(lambda x : 1 if ((x['ending_inventory_units']>0) & (x['inventory_units_sold']==0)) else 0, axis=1)
#         inventory_weekly_df['out_of_stock_weeks'] = inventory_weekly_df.apply(lambda x : 1 if (x['ending_inventory_units']==0) else 0, axis=1)

#         inventory_agg = inventory_weekly_df.groupby(['product_variant_sku'], as_index=False).agg({'week':'count', 
#                                                                                                   'inventory_units_sold': sum,
#                                                                                                   'Active_Weeks': sum,
#                                                                                                   'Inactive_Weeks': sum,
#                                                                                                   'out_of_stock_weeks': sum })
#         active_weeks = inventory_agg['Active_Weeks'].replace(0, pd.NA)
#         inventory_agg['avg_weekly_sales'] = (inventory_agg['inventory_units_sold'] / active_weeks).fillna(0)

#         inventory_agg_data = inventory_agg[['product_variant_sku', 'out_of_stock_weeks', 'avg_weekly_sales']].reset_index(drop=True)

#         return inventory_agg_data


def get_sales_by_channel_df(table_data):        
       
        product_title = list(map(itemgetter('product_title'), table_data['rows']))
        sales_channel = list(map(itemgetter('sales_channel'), table_data['rows']))
        orders = list(map(itemgetter('orders'), table_data['rows']))
        quantity_returned = list(map(itemgetter('quantity_returned'), table_data['rows']))
        net_sales = list(map(itemgetter('net_sales'), table_data['rows']))
        average_order_value = list(map(itemgetter('average_order_value'), table_data['rows']))

        channel_sales_dict = {'product_title': product_title,
                'sales_channel': sales_channel,
                'orders': orders,
                'quantity_returned': quantity_returned,               
                'net_sales': net_sales,
                'average_order_value': average_order_value}
        channel_sales_df = pd.DataFrame(channel_sales_dict)

        channel_sales_df['orders']=channel_sales_df['orders'].astype(int)
        channel_sales_df['quantity_returned']=channel_sales_df['quantity_returned'].astype(int)
        channel_sales_df['net_sales']=channel_sales_df['net_sales'].astype(float)
        channel_sales_df['average_order_value']=channel_sales_df['average_order_value'].astype(float)

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

        inventory_channel_dict = {'product_title': product_title,
                                'product_variant': product_variant_title,
                                'product_variant_sku': product_variant_sku,
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
        inventory_channel_df['out_of_stock_sku'] = inventory_channel_df.apply(lambda x : 1 if (x['ending_inventory_units']==0) else 0, axis=1) 

        inventory_channel_df = inventory_channel_df.reset_index(drop=True)
        return inventory_channel_df


def get_consolidated_df(df_list):

    consolidated_df = functools.reduce(lambda left, right: pd.merge(left, right, on='product_variant_sku', how='left'), df_list)
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
