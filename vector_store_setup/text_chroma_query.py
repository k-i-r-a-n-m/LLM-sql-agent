import chromadb

chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="sql_schema")


# Function to query ChromaDB and retrieve schema information
def query_schema(user_query: str, n_results: int = 5):
    results = collection.query(
        query_texts=[user_query], 
        n_results=n_results
    )
    
    # Extract relevant results
    retrieved_docs = results.get("documents", [[]])[0]  # Get top n_results
    
    return retrieved_docs if retrieved_docs else ["No relevant schema found."]

# **Test the Chroma Query**
# test_query = "Which table stores order details?"
test_query = "i need categories list, which table can be used"
retrieved_info = query_schema(test_query)

print("\nQuery:", test_query)
print("Retrieved Schema Information:")
for doc in retrieved_info:
    print("-", doc)
