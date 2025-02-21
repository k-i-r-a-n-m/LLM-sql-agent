from pprint import pprint
import chromadb

chroma_client = chromadb.PersistentClient(path="./chroma_db")  # Ensure schema is stored here
db_collection = chroma_client.get_collection(name="sql_schema")


def hierarical_retriver(user_query):
    # Retrieve relevant tables
    table_results = db_collection.query(
        query_texts=[user_query], n_results=3, where={"type": "table"}, include=["distances", "metadatas"]
    )

    tables = [
        (doc["table_name"], score) 
        for doc, score in zip(table_results.get("metadatas", [])[0], table_results.get("distances", [])[0])
    ]

    # Retrieve relevant columns
    column_results = db_collection.query(
        query_texts=[user_query],
        n_results=3,
        where={"$and": [{"type": {"$eq": "column"}}, {"table": {"$in": [table[0] for table in tables]}}]},
        include=["distances", "metadatas"]
    )

    columns = [
        (doc["table"], doc["columns"], score) 
        for doc, score in zip(column_results.get("metadatas", [])[0], column_results.get("distances", [])[0])
    ]

    # Retrieve table relationships
    relationship_results = db_collection.query(
        query_texts=[user_query], n_results=3, where={"type": "relationship"}, include=["distances", "metadatas"]
    )

    relationships = [
        (doc["table1"], doc["table2"], doc["relationship_type"], score) 
        for doc, score in zip(relationship_results.get("metadatas", [])[0], relationship_results.get("distances", [])[0])
    ]

    print("\n\n------------------------------- RAG DATA ---------------------------- \n")
    pprint({
        "tables": tables,
        "columns": columns,
        "table_relationships": relationships
    })

    return {
        "tables": tables,
        "columns": columns,
        "table_relationships": relationships
    }


if __name__ == "__main__":
    user_query = "list me all the customers"
    hierarical_retriver(user_query)
