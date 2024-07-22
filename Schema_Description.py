from sqlalchemy import create_engine, inspect


def get_database_schema(connection_string):
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    schema = {}

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        schema[table_name] = {
            "columns": [f"{col['name']} ({col['type']})" for col in columns],
            "relationships": []  # Add logic to extract relationships if necessary
        }

    return schema


def format_schema_description(schema):
    description = "The database has the following tables:\n"
    for table, details in schema.items():
        description += f"Table: {table}\nColumns: {', '.join(details['columns'])}\n"
        if details['relationships']:
            description += f"Relationships: {', '.join(details['relationships'])}\n"
        description += "\n"
    return description