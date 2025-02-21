import pandas as pd
from sqlalchemy import create_engine , text


def create_db():

    # Create an in-memory SQLite database
    # engine = create_engine("sqlite:///:memory:", echo=True)

    # Create a persistent SQLite database in a file
    engine = create_engine("sqlite:///electrical_parts.db", echo=True)

    # 1. Define data for suppliers
    suppliers_df = pd.DataFrame({
        "supplier_id": [1, 2, 3],
        "name": ["ElectroSupply Inc.", "Voltage Solutions", "Wattage Wholesale"],
        "contact_name": ["John Doe", "Jane Smith", "Mike Johnson"],
        "phone": ["555-1234", "555-5678", "555-9101"],
        "address": ["123 Electric Ave", "456 Current St", "789 Circuit Rd"]
    })

    # 2. Define data for categories
    categories_df = pd.DataFrame({
        "category_id": [1, 2, 3, 4],
        "name": ["Wires & Cables", "Switches & Sockets", "Lighting", "Transformers"]
    })

    # 3. Define data for products
    products_df = pd.DataFrame({
        "product_id": [1, 2, 3, 4],
        "name": ["Copper Wire 10m", "LED Light Bulb 10W", "Electrical Socket", "Mini Transformer 220V-110V"],
        "category_id": [1, 3, 2, 4],
        "supplier_id": [1, 2, 3, 1],
        "price": [25.99, 5.49, 3.75, 45.99],
        "stock_quantity": [100, 500, 300, 50]
    })

    # 4.Define data for customers
    customers_df = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Alice Cooper", "Bob Martin"],
        "email": ["alice@email.com", "bob@email.com"],
        "phone": ["555-1111", "555-2222"],
        "address": ["101 Main St", "202 Elm St"]
    })

    # 5. Define data for orders
    orders_df = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [1, 2],
        "order_date": ["2024-02-10", "2024-02-15"],
        "total_amount": [51.48, 49.74]
    })

    # 6. Define data for order items
    order_items_df = pd.DataFrame({
        "order_item_id": [1, 2, 3],
        "order_id": [1, 2, 2],
        "product_id": [1, 2, 3],
        "quantity": [2, 3, 2],
        "unit_price": [25.99, 5.49, 3.75]
    })

    # Write DataFrames to SQLite
    suppliers_df.to_sql("suppliers", con=engine, index=False, if_exists="replace")
    categories_df.to_sql("categories", con=engine, index=False, if_exists="replace")
    products_df.to_sql("products", con=engine, index=False, if_exists="replace")
    customers_df.to_sql("customers", con=engine, index=False, if_exists="replace")
    orders_df.to_sql("orders", con=engine, index=False, if_exists="replace")
    order_items_df.to_sql("order_items", con=engine, index=False, if_exists="replace")


    # return engine.connect()

    # # Query Example: Fetch all products
    # with engine.connect() as conn:
    #     result = conn.execute(text("SELECT * FROM products")).fetchall()
    #     print("\nProducts Table:")
    #     for row in result:
    #         print(row)

# # Query Example: Join orders and customers
# query = """
# SELECT o.order_id, c.name AS customer_name, o.order_date, o.total_amount
# FROM orders o
# JOIN customers c ON o.customer_id = c.customer_id;
# """

# with engine.connect() as conn:
#     result = conn.execute(text(query)).fetchall()
#     print("\nOrders with Customers:")
#     for row in result:
#         print(row)

if __name__ == "__main__":
    create_db()