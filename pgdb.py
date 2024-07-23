from dotenv import load_dotenv
import Schema_Description as schema
import streamlit as st
import os
import psycopg2
import google.generativeai as genai
import pandas as pd

# Load environment variables
load_dotenv()

# Configure Genai Key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Function to load Google Gemini Model and provide queries as response
def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text

# Function to retrieve table column names
def get_table_columns(table_name):
    conn_params = {
        'dbname': 'ebxdb',
        'user': 'postgres',
        'password': 'ayush',
        'host': 'localhost',
        'port': '5432'  # Default port for PostgreSQL
    }
    
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE LOWER(table_name) = LOWER('{table_name}');
    """
    
    columns = []
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(query)
        columns = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
    
    return columns

test = " *"
# Function to replace SELECT * with specific columns
def replace_select_all(sql):
    
    print(test in sql)
    if test in sql:
        print("======")
        table_name = sql.split('FROM')[1].split()[0].strip()
        table_name = table_name.replace(";","")
        print(table_name)
        columns = get_table_columns(table_name)
        print(type(columns))
        print(columns)
        elements_to_remove = ['t_last_user_id', 't_creator_id', 't_creation_date', 't_last_write']

# Remove each element in elements_to_remove from my_list
        for element in elements_to_remove:
            if element in columns:
                columns.remove(element)
        if columns:
            columns_str = ', '.join(columns)
            sql = sql.replace(test, f' {columns_str}')
            print(sql)
    
    return sql

# Function to retrieve query from the PostgreSQL database
def read_sql_query(sql):
    conn_params = {
        'dbname': 'EbxEvaX_Base',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'  # Default port for PostgreSQL
    }

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        # Get column names
        colnames = [desc[0] for desc in cur.description]

        # Convert rows to DataFrame
        df = pd.DataFrame(rows, columns=colnames)

        # Close the cursor and connection
        cur.close()
        conn.close()
    except Exception as e:
        print(e)
        return pd.DataFrame()

    return df

# Get schema details
schemadetails = schema.get_database_schema("postgresql://postgres:ayush@127.0.0.1:5432/ebxdb")
description = schema.format_schema_description(schemadetails)

# Define your prompt
prompt = [
    """
    You are an expert in converting English questions to SQL queries for a PostgreSQL database!
The Postgres database has the following schema and data description: 
{}, which includes table names, columns, and relationships between tables.
The database includes an "Age Group" table structured as follows:
Age Group Id | Age Category
-------------|-------------
1            | <30
2            | 30-49
3            | 50+
Here are examples of how to convert English questions to PostgreSQL queries:

Example 1 - 
English Question: What are the names of all the tables in the database?
PostgreSQL Query: SELECT table_name
                  FROM information_schema.tables
                  WHERE LOWER(table_schema) = LOWER('public')
                  AND LOWER(table_type) = LOWER('BASE TABLE');

Example 2 - 
English Question: Tell me all the employees whose gender is male?
PostgreSQL Query: SELECT *
                  FROM ebx_employee 
                  WHERE LOWER(gender)=LOWER('Male');

Example 3 - 
English Question: employees who are born in 1985?
PostgreSQL Query: SELECT *
                  FROM ebx_employee
                  WHERE TO_CHAR(date00, 'YYYY') = '1985';
Example 4 - 
English Question: List all employees with a salary greater than $50,000.
PostgreSQL Query: SELECT *
                  FROM ebx_employee
                  WHERE salary > 50000;

Example 5 - 
English Question: Show the total number of employees.
PostgreSQL Query: SELECT COUNT(*)
                  FROM ebx_employee;

Example 6 - 
English Question: Retrieve the names and emails of all employees.
PostgreSQL Query: SELECT name, email
                  FROM ebx_employee;

Example 7 - 
English Question: Find all employees who joined in the last 30 days.
PostgreSQL Query: SELECT *
                  FROM ebx_employee
                  WHERE join_date >= NOW() - INTERVAL '30 days';

Example 8 - 
English Question: Show all employees who work in the sales department.
PostgreSQL Query: SELECT *
                  FROM ebx_employee
                  WHERE LOWER(department)=LOWER('sales');

Example 9 - 
English Question: List the names and job titles of all employees who were promoted after 2019.
PostgreSQL Query: SELECT name, job_title
                  FROM ebx_employee
                  WHERE promotion_date > '2019-12-31';

Example 10 - 
English Question: Display the average salary of all employees.
PostgreSQL Query: SELECT AVG(salary)
                  FROM ebx_employee;

Example 11 - 
English Question: What are the names of the employees from the HR department?
PostgreSQL Query: SELECT name
                  FROM ebx_employee
                  WHERE LOWER(department) = LOWER('HR');

Example 12 - 
English Question: Give me the details of all employees who have more than 10 years of experience.
PostgreSQL Query: SELECT *
                  FROM ebx_employee
                  WHERE experience_years > 10;

Note:
- The SQL query should be formatted for PostgreSQL.
- Use case-insensitive comparisons where applicable.
- Do not include the words "in beginning or end" and "sql" in the output.
- Ensure the SQL code is accurate and syntactically correct for PostgreSQL.
- If the user asks what data is present, create a query that presents the entire datatable to the user. 
    """.format(description)
]

# Helper function to clean the response
def starts_with_sql(s):
    if s.startswith("```sql"):
        return s[6:-4]
    return s

# Streamlit App configuration
st.set_page_config(page_title="I can Retrieve Any SQL query")
st.header("EVaX")

# Streamlit form for user input
with st.form(key='query_form'):
    question = st.text_input("Ask Your Question Below: ", key="input")
    submit = st.form_submit_button("Submit")

# If submit is clicked
if submit:
    response = get_gemini_response(question, prompt)
    query = starts_with_sql(response)
    print(query)
    query1 = replace_select_all(query)
    df = read_sql_query(query1)
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    if not df.empty:
        st.dataframe(df)
    else:
        st.write("Data not found in the database!")
