from datetime import datetime
from dateutil.relativedelta import relativedelta


## Six-Month window dates
today = datetime.today().date()
## Start date: First day of the month, 6 months ago
start_date = (today - relativedelta(months=6)).replace(day=1)
## End date: Last day of previous month
end_date = today.replace(day=1) - relativedelta(days=1)


# Query-1
def get_sales_query():
    """
    Get Query for Top-10 SKU by sales in last 14 days.
    """
    final_query = fr"""
        FROM sales
        SHOW orders, net_sales, average_order_value
        WHERE cost_is_recorded = true
        GROUP BY product_title, product_variant_title, product_variant_sku
        SINCE startOfDay(-15d) UNTIL yesterday
        ORDER BY net_sales DESC
        LIMIT 10
        """
    
    return final_query


# Query-2
def get_inventory_query(sku_list):
    """
    Get inventory query for given product list.    
    Args:
        sku_list: Comma-separated tuple of product SKUs
    """
    final_query = fr"""
        FROM inventory
        SHOW inventory_units_sold, ending_inventory_units, percent_of_inventory_sold,
            sell_through_rate
        WHERE inventory_is_tracked = true
        AND product_variant_sku IN {sku_list}
        GROUP BY product_variant_sku WITH TOTALS
        SINCE startOfDay(-14d) UNTIL yesterday
        ORDER BY product_variant_sku ASC
        """
    
    return final_query


# Query-3
def get_inventory_weekly_query(sku_list):
    """
    Get inventory query for given product list.    
    Args:
        [ 6-month period ]
        start_date: Start date for the query 
        end_date: End date for the query
        sku_list: Comma-separated tuple of product SKUs
    """
    final_query = fr"""
        FROM inventory
        SHOW week, inventory_units_sold, ending_inventory_units
        WHERE inventory_is_tracked = true
        AND product_variant_sku IN {sku_list}
        GROUP BY week, product_title, product_variant_title, product_variant_sku
        SINCE {start_date} UNTIL {end_date}
        ORDER BY product_variant_sku, week ASC
        """
    
    return final_query


# Query-4
def get_channel_sales_query():
    """
    Get net-sales by channel query (TikTok & Meta).    
    Args:
        [ 6-month period ]
        start_date: Start date for the query 
        end_date: End date for the query
        sku_list: Comma-separated tuple of product SKUs
    """    
    final_query = fr"""
        FROM sales
        SHOW orders, quantity_returned, net_sales, average_order_value
        WHERE sales_channel IN ('TikTok', 'Facebook & Instagram')
        GROUP BY product_title, sales_channel
        SINCE {start_date} UNTIL {end_date}
        ORDER BY net_sales DESC
        LIMIT 5
        """
    
    return final_query


# Query-5
def get_channel_inventory_query(product_list):
    """
    Get inventory for top products by channel (TikTok & Meta).    
    Args:
        [ 6-month period ]
        start_date: Start date for the query 
        end_date: End date for the query
        sku_list: Comma-separated tuple of product SKUs
    """    
    final_query = fr"""
        FROM inventory
        SHOW inventory_units_sold, ending_inventory_units, days_out_of_stock, sell_through_rate
        WHERE inventory_is_tracked = true
        AND product_title IN {product_list}
        GROUP BY product_title, product_variant_title, product_variant_sku
        SINCE {start_date} UNTIL {end_date}
        """
    
    return final_query