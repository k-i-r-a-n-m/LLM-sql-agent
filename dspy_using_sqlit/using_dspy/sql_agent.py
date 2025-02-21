import os
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
    engine = create_engine("sqlite:///my_database.db", echo=True)
    return engine.connect()

DB_CONN = create_db_connection()

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
        with DB_CONN.begin() as conn:
            result = conn.execute(text(query)).fetchall()
        return result
    except Exception as e:
        return {"error": str(e)}

class RetrieveSchema(dspy.Module):
    def forward(self, user_query: str):
        """Performs hierarchical retrieval of schema, including table relationships."""
        
        # Retrieve relevant tables
        table_results = db_collection.query(query_texts=[user_query], n_results=5, metadata_filter={"type": "table"})
        tables = [doc['table_name'] for doc in table_results.get('metadatas', [])]
        
        # Retrieve relevant columns
        column_results = db_collection.query(query_texts=[user_query], n_results=10, metadata_filter={"type": "column", "table": {"$in": tables}})
        columns = [(doc['table'], doc['column_name']) for doc in column_results.get('metadatas', [])]
        
        # Retrieve table relationships
        relationship_results = db_collection.query(query_texts=[user_query], n_results=5, metadata_filter={"type": "relationship"})
        relationships = [(doc['table1'], doc['table2'], doc['relationship_type']) for doc in relationship_results.get('metadatas', [])]
        
        return tables, columns, relationships

class GenerateSQL(dspy.Signature):
    question: str = dspy.InputField()
    sql_query: str = dspy.OutputField()
    result: list = dspy.OutputField()
    answer: str = dspy.OutputField(desc="Answer to the user's question")
    summary: str = dspy.OutputField(
        desc="Summary should be based on the user's question and include query details if any"
    )

sql_query_generator = dspy.ReAct(
    GenerateSQL,
    tools=[
        execute_sql,
    ],
)

def sql_agent(user_query: str):
    retrieve_schema = RetrieveSchema()
    tables, columns, relationships = retrieve_schema(user_query)
    
    response = sql_query_generator(question=user_query)
    
    return response

def main():
    st.title("SQL Chatbot")
    user_query = st.text_input("Enter your query:")
    if st.button("Submit"):
        response = sql_agent(user_query)
        
        st.subheader("Generated SQL Query:")
        st.code(response.sql_query, language="sql")
        
        st.subheader("Query Results:")
        st.write(response.result)
        
        st.subheader("Summary:")
        st.write(response.summary)
        
        st.subheader("Answer:")
        st.write(response.answer)

if __name__ == "__main__":
    main()
