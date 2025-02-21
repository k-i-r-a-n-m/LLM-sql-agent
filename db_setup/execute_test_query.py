# from using_sqlit import create_db_connection
from sqlalchemy import create_engine , text

# db_engine = create_db_connection()

engine = create_engine("sqlite:///electrical_parts.db")


with engine.connect() as conn:
        result = conn.execute(text("SELECT customer_id FROM customers WHERE name = 'Alice Cooper';")).fetchall()
        print("\nProducts Table:")
        for row in result:
            print(row)