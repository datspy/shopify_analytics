from datetime import datetime
from dateutil.relativedelta import relativedelta


## Six-Month window dates
today = datetime.today().date()
## Start date: First day of the month, 6 months ago
start_date = (today - relativedelta(months=6)).replace(day=1)
## End date: Last day of previous month
end_date = today.replace(day=1) - relativedelta(days=1)


# # Query-1
def get_top_selling_query():
    """
    Get Query for Top-10 SKU by sales in last 14 days.
    """
    final_query = fr"""
        FROM sales
        SHOW orders, net_sales, average_order_value
        WHERE cost_is_recorded = true
        GROUP BY product_title, product_variant_title, product_variant_sku
        SINCE startOfDay(-14d) UNTIL yesterday
        ORDER BY net_sales DESC
        LIMIT 10
        """
    
    return final_query


# # Query-2
def get_inventory_query(sku_list):
    """
    Get inventory query for given product list.    
    Args:
        sku_list: Comma-separated tuple of product SKUs
    """
    final_query = fr"""
        FROM inventory
        SHOW inventory_units_sold, ending_inventory_units
        WHERE inventory_is_tracked = true        
        GROUP BY product_variant_sku
        HAVING inventory_units_sold > 0
        SINCE startOfDay(-60d) UNTIL yesterday
        ORDER BY product_variant_sku ASC
        """
    
    return final_query


# # Query-3
def get_inventory_agg_query():
    """
    Get inventory query from BigQuery table.    
    Args:
        [ 6-month period ]
        start_date: Start date for the query 
        end_date: End date for the query
    """
    final_query = fr"""
           select 
            product_variant_sku,             
            sum(active_weeks) as active_weeks, 
            sum(out_of_stock_weeks) as out_of_stock_weeks, 
            round(sum(inventory_units_sold)/sum(active_weeks),2) as avg_weekly_sales
                from `upwork-478017.fith_shopify_analytics.all_sku_weekly_inventory_data`
            group by 1
            order by avg_weekly_sales desc;
        """
    
    return final_query


# Query-3
def get_all_sku_channel_sales_query():
    """
    Get net-sales by channel query (TikTok & Meta).    
    Args:
        [ 60-day period ]
    """    
    final_query = fr"""
        FROM sales
        SHOW orders, net_sales
        WHERE sales_channel IN ('TikTok', 'Facebook & Instagram')
        GROUP BY product_variant_sku
        HAVING net_sales > 0
        SINCE startOfDay(-60d) UNTIL yesterday
        ORDER BY net_sales DESC        
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
        HAVING inventory_units_sold > 0
        SINCE startOfMonth(-2m) UNTIL endOfMonth(- 1m)
        """
    
    return final_query


# Query-6
def get_referring_channel_query():
    """
    Get sales by referring channel query.
    Args:
        [ 60-day period ]
    """    
    final_query = fr"""
        FROM sales
        SHOW orders, gross_sales, net_sales, discounts
        WHERE cost_is_recorded = true
            AND sales_channel = 'Online Store'
            AND referring_channel != 'direct'
            AND referring_channel IS NOT NULL
        GROUP BY referring_channel
        SINCE startOfDay(-60d) UNTIL today
        ORDER BY gross_sales DESC
        """
    
    return final_query


# Query-7
def get_all_sku_sales_query():
    """
    Get all sku-sales query.
    Args:
        [ 60-day period ]
    """    
    final_query = fr"""
        FROM sales
        SHOW orders, net_sales, average_order_value
        WHERE cost_is_recorded = true
        GROUP BY product_title, product_variant_title, product_variant_sku
        HAVING net_sales > 0
        SINCE startOfDay(-60d) UNTIL yesterday
        ORDER BY net_sales DESC
        """
    
    return final_query


# Query-8
def get_all_sku_inventory_query():
    """
    Get all sku-inventory query.
    Args:
        [ 60-day period ]
    """    
    final_query = fr"""
        FROM inventory
        SHOW inventory_units_sold, ending_inventory_units
        WHERE inventory_is_tracked = true
            AND product_variant_sku IS NOT NULL
        GROUP BY product_title, product_variant_title, product_variant_sku
        HAVING inventory_units_sold > 0
        DURING last_year
        ORDER BY ending_inventory_units DESC
        """
    
    return final_query


# Query-9
def get_all_sku_weekly_inventory_query(sku_list):
    """
    Get all sku-weeklyinventory query.
    Args:
        [ 60-day period ]
    """    
    final_query = fr"""
        FROM inventory
        SHOW week, inventory_units_sold, ending_inventory_units
        WHERE inventory_is_tracked = true
        AND product_variant_sku IN {sku_list}
        GROUP BY week, product_variant_sku
        DURING last_year
        ORDER BY product_variant_sku, week ASC
        """
    
    return final_query