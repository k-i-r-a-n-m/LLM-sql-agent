from pprint import pprint
import chromadb

chroma_client = chromadb.PersistentClient(
    path="./chroma_db"
)  # Ensure schema is stored here
db_collection = chroma_client.get_collection(name="sql_schema")


def hierarical_retriver(user_query):
    # Retrieve relevant tables
    table_results = db_collection.query(
        query_texts=[user_query], n_results=3, where={"type": "table"}
    )
    tables = [doc["table_name"] for doc in table_results.get("metadatas", [])[0]]
    

    # Retrieve relevant columns
    column_results = db_collection.query(
        query_texts=[user_query],
        n_results=3,
        where={"$and": [{"type": {"$eq": "column"}}, {"table": {"$in": tables}}]},
    )
    
    columns = [ (doc["table"], doc["columns"]) for doc in column_results.get("metadatas", [])[0]  ]
    

    # Retrieve table relationships
    relationship_results = db_collection.query(
        query_texts=[user_query], n_results=3, where={"type": "relationship"}
    )
    relationships =  [
            (doc["table1"], doc["table2"], doc["relationship_type"])
            for doc in relationship_results.get("metadatas", [])[0]
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
    # user_query = "i need categories list, which table can be used"
    user_query = "list me all the customers"
    hierarical_retriver(user_query)
