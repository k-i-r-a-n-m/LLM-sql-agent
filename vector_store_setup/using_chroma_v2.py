import os
import chromadb

# Initialize ChromaDB with persistence
chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="sql_schema")  # Uses default embedding model

# Define schema chunks with tables and relationships
schema_chunks = [
    # Tables
    {"id": "suppliers_table", "text": "table: suppliers", "metadata": {"type": "table","table_name":"suppliers"}},
    {"id": "categories_table", "text": "table: categories", "metadata": {"type": "table","table_name":"categories"}},
    {"id": "products_table", "text": "table: products", "metadata": {"type": "table","table_name":"products"}},
    {"id": "customers_table", "text": "table: customers", "metadata": {"type": "table","table_name":"customers"}},
    {"id": "orders_table", "text": "table: orders", "metadata": {"type": "table","table_name":"orders"}},
    {"id": "order_items_table", "text": "table: order_items", "metadata": {"type": "table","table_name":"order_items"}},
    
    # Columns
    {"id": "suppliers_columns", "text": "Columns: supplier_id, name, contact_name, phone, address", "metadata": {"type": "column", "table": "suppliers", "columns": "supplier_id, name, contact_name, phone, address"}},
    {"id": "categories_columns", "text": "Columns: category_id, name", "metadata": {"type": "column", "table": "categories", "columns": "category_id, name"}},
    {"id": "products_columns", "text": "Columns: product_id, name, category_id, supplier_id, price, stock_quantity", "metadata": {"type": "column", "table": "products", "columns": "product_id, name, category_id, supplier_id, price, stock_quantity"}},
    {"id": "customers_columns", "text": "Columns: customer_id, name, email, phone, address", "metadata": {"type": "column", "table": "customers", "columns": "customer_id, name, email, phone, address"}},
    {"id": "orders_columns", "text": "Columns: order_id, customer_id, order_date, total_amount", "metadata": {"type": "column", "table": "orders", "columns": "order_id, customer_id, order_date, total_amount"}},
    {"id": "order_items_columns", "text": "Columns: order_item_id, order_id, product_id, quantity, unit_price", "metadata": {"type": "column", "table": "order_items", "columns": "order_item_id, order_id, product_id, quantity, unit_price"}},
    
    # Relationships
    {"id": "products_suppliers_relationship", "text": "Relationship: products.supplier_id → suppliers.supplier_id", "metadata": {"type": "relationship", "table1": "products", "table2": "suppliers", "relationship_type": "foreign_key"}},
    {"id": "products_categories_relationship", "text": "Relationship: products.category_id → categories.category_id", "metadata": {"type": "relationship", "table1": "products", "table2": "categories", "relationship_type": "foreign_key"}},
    {"id": "orders_customers_relationship", "text": "Relationship: orders.customer_id → customers.customer_id", "metadata": {"type": "relationship", "table1": "orders", "table2": "customers", "relationship_type": "foreign_key"}},
    {"id": "order_items_orders_relationship", "text": "Relationship: order_items.order_id → orders.order_id", "metadata": {"type": "relationship", "table1": "order_items", "table2": "orders", "relationship_type": "foreign_key"}},
    {"id": "order_items_products_relationship", "text": "Relationship: order_items.product_id → products.product_id", "metadata": {"type": "relationship", "table1": "order_items", "table2": "products", "relationship_type": "foreign_key"}},
]


# Check existing documents to avoid duplication
existing_ids = set(collection.get()["ids"])

for chunk in schema_chunks:
    if chunk["id"] not in existing_ids:
        collection.add(
            ids=[chunk["id"]],
            documents=[chunk["text"]],  # Chroma will automatically generate embeddings
            metadatas=[chunk["metadata"]]
        )

print("\n\nSchema stored persistently in ChromaDB.\n\n")

