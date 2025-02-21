import os
from pprint import pprint
import chromadb
import json
import dspy
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Configure DSPy Language Model
lm = dspy.LM("openai/gpt-4o-mini", api_key=os.getenv("OPENAI_API_KEY"))
dspy.configure(lm=lm)

# Initialize ChromaDB client
chroma_client = chromadb.PersistentClient(path="./chroma_db")  # Ensure schema is stored here
db_collection = chroma_client.get_collection(name="sql_schema")

# Create SQLite persistent database connection
def create_db_connection():
    engine = create_engine("sqlite:///electrical_parts.db", echo=True)
    return engine.connect()

# DB_CONN = create_db_connection()

def validate_sql_query(query: str) -> bool:
    """Validate that the query is a SELECT statement and is not empty."""
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError(
            "Only SELECT queries are allowed for safety. Please rephrase your question to get information instead of modifying data."
        )
    return True

def execute_sql(query: str):
    """Executes the SQL query in SQLite and fetches results."""
    try:
       

        validate_sql_query(query)
        with create_db_connection() as conn:
            result = conn.execute(text(query)).fetchall()
        return result
    except Exception as e:
        return {"error": str(e)}

class RetrieveSchema(dspy.Module):
    def forward(self, user_query: str):
        """Performs hierarchical retrieval: first tables, then columns, then relationships."""

        # Retrieve relevant tables
        table_results = db_collection.query(
            query_texts=[user_query], n_results=5, where={"type": "table"}
        )
        tables = [doc["table_name"] for doc in table_results.get("metadatas", [])[0]]
        

        # Retrieve relevant columns
        column_results = db_collection.query(
            query_texts=[user_query],
            n_results=5,
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
            "relationships": relationships
            })

        return {
            "tables": tables, 
            "columns": columns,
            "relationships": relationships
            }

class GenerateSQL(dspy.Signature):
    question: str = dspy.InputField()
    context: str = dspy.InputField()
    sql_query: str = dspy.OutputField()
    # result: list = dspy.OutputField()
    answer: str = dspy.OutputField(desc="Answer to the user's question based on the query being executed")
    # summary: str = dspy.OutputField(
    #     desc="Summary should be based on the user's question and execution result"
    # )

sql_query_generator = dspy.ReAct(
    GenerateSQL,
    tools=[
        execute_sql,
    ],
)

def sql_agent(user_query: str):
    retrieve_schema = RetrieveSchema()
    query_context = retrieve_schema(user_query)
    
    response = sql_query_generator(question=user_query,context=query_context)
    
    return response



if __name__ == "__main__":
    # main()
    # user_query = " list me all the customers names"
    user_query = "list the stock quantity of product id 1 with price and it's category name"
    # user_query = "what is the total amount for customer alice cooper"
    # user_query = "what are the products purchased by customer bob martin"
    # user_query = "what is the supplier name of product id '1' and give his address"
    # user_query = "what is the supplier name of product Electrical Socket and give his address also state the category of the product list the customer name purchased the product and total amount for overall order"
    # user_query = "delete the customers table"
    response = sql_agent(user_query)

    print(response)
