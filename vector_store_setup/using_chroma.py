import os
import chromadb

# Initialize ChromaDB with persistence
chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="sql_schema")  # Uses default embedding model

# Define schema chunks with tables and relationships
schema_chunks = [
    {"id": "suppliers_table", "text": "Table: suppliers\nColumns: supplier_id, name, contact_name, phone, address"},
    {"id": "categories_table", "text": "Table: categories\nColumns: category_id, name"},
    {"id": "products_table", "text": "Table: products\nColumns: product_id, name, category_id, supplier_id, price, stock_quantity"},
    {"id": "customers_table", "text": "Table: customers\nColumns: customer_id, name, email, phone, address"},
    {"id": "orders_table", "text": "Table: orders\nColumns: order_id, customer_id, order_date, total_amount"},
    {"id": "order_items_table", "text": "Table: order_items\nColumns: order_item_id, order_id, product_id, quantity, unit_price"},
    
    # Relationships
    {"id": "products_suppliers_relationship", "text": "Relationship: products.supplier_id → suppliers.supplier_id"},
    {"id": "products_categories_relationship", "text": "Relationship: products.category_id → categories.category_id"},
    {"id": "orders_customers_relationship", "text": "Relationship: orders.customer_id → customers.customer_id"},
    {"id": "order_items_orders_relationship", "text": "Relationship: order_items.order_id → orders.order_id"},
    {"id": "order_items_products_relationship", "text": "Relationship: order_items.product_id → products.product_id"},
]

# Check existing documents to avoid duplication
existing_ids = set(collection.get()["ids"])

for chunk in schema_chunks:
    if chunk["id"] not in existing_ids:
        collection.add(
            ids=[chunk["id"]],
            documents=[chunk["text"]]  # Chroma will automatically generate embeddings
        )

print("Schema stored persistently in ChromaDB.")

