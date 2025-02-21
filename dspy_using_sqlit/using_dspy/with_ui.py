import os
import streamlit as st
import chromadb
import dspy
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure DSPy Language Model
lm = dspy.LM("openai/gpt-4o-mini", api_key=os.getenv("OPENAI_API_KEY"))
dspy.configure(lm=lm)

# Initialize ChromaDB client
chroma_client = chromadb.PersistentClient(path="./chroma_db")
db_collection = chroma_client.get_collection(name="sql_schema")

# Create SQLite database connection
def create_db_connection():
    engine = create_engine("sqlite:///electrical_parts.db", echo=True)
    return engine.connect()

def validate_sql_query(query: str) -> bool:
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed.")
    return True

def execute_sql(query: str):
    try:
        validate_sql_query(query)
        with create_db_connection() as conn:
            result = conn.execute(text(query)).fetchall()
        return result
    except Exception as e:
        return {"error": str(e)}

class RetrieveSchema(dspy.Module):
    def forward(self, user_query: str):
        table_results = db_collection.query(query_texts=[user_query], n_results=3, where={"type": "table"})
        tables = [doc["table_name"] for doc in table_results.get("metadatas", [])[0]]
        
        column_results = db_collection.query(query_texts=[user_query], n_results=3, where={"$and": [{"type": {"$eq": "column"}}, {"table": {"$in": tables}}]})
        columns = [(doc["table"], doc["columns"]) for doc in column_results.get("metadatas", [])[0]]
        
        relationship_results = db_collection.query(query_texts=[user_query], n_results=3, where={"type": "relationship"})
        relationships = [(doc["table1"], doc["table2"], doc["relationship_type"]) for doc in relationship_results.get("metadatas", [])[0]]
        
        return {"tables": tables, "columns": columns, "relationships": relationships}

class GenerateSQL(dspy.Signature):
    question: str = dspy.InputField()
    context: str = dspy.InputField()
    sql_query: str = dspy.OutputField()
    answer: str = dspy.OutputField()

sql_query_generator = dspy.ReAct(GenerateSQL, tools=[execute_sql])

def sql_agent(user_query: str):
    retrieve_schema = RetrieveSchema()
    query_context = retrieve_schema(user_query)
    response = sql_query_generator(question=user_query, context=query_context)
    return response

st.title("SQL Chatbot")
st.subheader("LLM : gpt-4o-mini (openai)")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

user_query = st.text_input("Enter your query:")
if st.button("Submit"):
    if user_query:
        response = sql_agent(user_query)
        query_result = execute_sql(response.sql_query)
        
        st.session_state.chat_history.append((user_query, response.answer, response.sql_query, query_result))

st.subheader("Chat History")
for i, (query, answer, sql, result) in enumerate(st.session_state.chat_history):
    with st.expander(f"Query {i+1}: {query}"):
        st.text_area("Generated SQL Query:", sql, height=100)
        st.write("Query Results:", result)
        st.write("Answer:", answer)
