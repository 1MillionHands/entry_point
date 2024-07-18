from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData, asc, desc, Column, Integer, String, ForeignKey, inspect, Table
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, scoped_session
from DB_Manager_EP.connectors.s3_connector import S3Connector
import json
import pandas as pd
import psycopg2
import os

# Get the absolute path to the current script
current_dir = os.path.dirname(__file__)

# Construct the path to the config file relative to the current script
config_path = os.path.join(current_dir, '..', 'config_file.json')

# Open and read the config file
with open(config_path, 'r') as f:
  config_data = json.load(f)
# with open('./config_file.json', 'r') as f:
#   config_data = json.load(f)
# with open(r'C:\Users\yanir\PycharmProjects\oneMilion\entry_point\DB_Manager_EP\config_file_.json', 'r') as f:
#   config_data = json.load(f)
#
# # todo: organize the file such that at the beginning there would the class utility function (i.e get db, check_table_exists and etc) and on the bottom all of the user usage function
#
class DbService:
    def __init__(self, is_test = True):
        self.engine = self.create_local_engine(test=is_test)
        self.SCHEMA = "omh_schema_test" if is_test else "omh_schema"
        # Create a session object to interact with the database
        self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

    @staticmethod
    def create_local_engine(test=False) -> object:
        secret = S3Connector.get_secret()
        HOST = config_data['aws_db']['host']
        PORT = config_data['aws_db']['port']
        DBNAME = config_data['aws_db']['dbname']
        SCHEMA = "omh_schema_test" if test else "omh_schema"
        USER = secret['username']
        PASSWORD = secret['password']

        # Define the database connection URL
        DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?options=-csearch_path%3D{SCHEMA}'
        print("Connecting to DB with URL:", DATABASE_URL)
        # Create a SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        return engine

    @contextmanager
    def get_db(self):
        session = self.Session()
        try:
            # session.execute(text(f'SET search_path TO {self.SCHEMA}'))
            yield session
            session.commit()
        finally:
            session.close()

    def create_table(self, tbl_obj):
        import logging

        # logging.basicConfig(level=logging.INFO)
        # logger = logging.getLogger('sqlalchemy.engine')
        # logger.setLevel(logging.INFO)

        if not self.engine.dialect.has_table(self.engine.connect(), tbl_obj.__tablename__):
            tbl_obj.metadata.create_all(self.engine)
            print(f"Table {tbl_obj.__tablename__} created successfully")
        else:
            print("Table name already exists in the db")

    def create_schema(self, db, schema_name):
        # Check if the table exists
        try:
            self.session.execute(text("CREATE SCHEMA " + schema_name))
            self.session.commit()
        except Exception as f:
            print(f)

    def insert_table(self, tbl_obj, headers):
        with self.get_db() as session:
            try:
                session.bulk_insert_mappings(tbl_obj, headers)
                session.commit()
                # print(session.statement.compile(compile_kwargs={"literal_binds": True}))
            except Exception as e:
                session.rollback()
                raise e
        # with self.get_db() as session:
        #     session.bulk_insert_mappings(tbl_obj, headers)

    def apply_filters(self, query, table_name, filters):
        filter_list = []
        invalid_columns = []
        for filter_dict in filters:
            col = filter_dict.get("column")
            vals = filter_dict.get("values")
            op = filter_dict.get("operator", "in")
            if not hasattr(table_name, col):
                invalid_columns.append(col)
                continue
            column_attr = getattr(table_name, col)
            if op == "in":
                filter_list.append(column_attr.in_(vals))
            elif op == ">":
                filter_list.append(column_attr > vals)
            elif op == "<":
                filter_list.append(column_attr < vals)
            elif op == ">=":
                filter_list.append(column_attr >= vals)
            elif op == "<=":
                filter_list.append(column_attr <= vals)
            elif op == "=":
                filter_list.append(column_attr == vals)
            else:
                raise ValueError(f"Unsupported filter operator: {op}")
        return query.filter(*filter_list), invalid_columns

    def apply_sort(self, query, table_name, sort_by):
        invalid_columns = []
        for col, ascending in sort_by.items():
            if not hasattr(table_name, col):
                invalid_columns.append(col)
                continue
            column_attr = getattr(table_name, col)
            if ascending:
                query = query.order_by(column_attr.asc())
            else:
                query = query.order_by(column_attr.desc())
        return query, invalid_columns

    def paginate_query(self, query, chunk_size, limit, to_df, invalid_columns):
        try:
            offset = 0
            res_df = []

            while True:
                # Apply limit and chunk_size for pagination
                limited_query = query.offset(offset).limit(min(chunk_size, limit - offset) if limit is not None else chunk_size)

                if to_df:
                    # Fetch the chunk and convert to DataFrame
                    chunk_df = pd.read_sql(limited_query.statement, self.engine)
                    if chunk_df.empty:
                        break
                    res_df.append(chunk_df)
                else:
                    # Fetch the chunk as list of SQLAlchemy objects
                    results = limited_query.all()
                    if not results:
                        break
                    res_df.extend(results)

                offset += chunk_size
                if limit is not None and offset >= limit:
                    break

            if to_df:
                # Concatenate all DataFrames in the list
                final_df = pd.concat(res_df, ignore_index=True)
                return final_df, invalid_columns
            else:
                return res_df, invalid_columns

        except Exception as e:
            print(f"An error occurred during pagination: {str(e)}")
            return None, invalid_columns

    def query_table_orm(self, table_name, columns=None, chunk_size=10000, limit=None, sort_by=None, filters=None, distinct=False, to_df=False):
        try:
            with self.get_db() as session:
                # Start building the query for the specified table using SQLAlchemy ORM
                invalid_columns = []  # Initialize an empty list for invalid columns

                if columns is None:
                    query = session.query(table_name)
                else:
                    # Check if all specified columns exist
                    invalid_columns = [col for col in columns if not hasattr(table_name, col)]
                    valid_columns = [col for col in columns if col not in invalid_columns]
                    if invalid_columns:
                        print(f"Invalid column(s): {', '.join(invalid_columns)}")
                    query = session.query(*[getattr(table_name, col) for col in valid_columns])

                # Apply filtering if specified
                if filters is not None:
                    query, invalid_cols_filters = self.apply_filters(query, table_name, filters)
                    invalid_columns.extend(invalid_cols_filters)

                # Apply sorting if specified
                if sort_by is not None:
                    query, invalid_cols_sort = self.apply_sort(query, table_name, sort_by)
                    invalid_columns.extend(invalid_cols_sort)

                # Apply distinct if specified
                if distinct:
                    query = query.distinct()

                # Fetch data in chunks using the pagination function
                results, invalid_columns = self.paginate_query(query, chunk_size, limit, to_df, invalid_columns)
                return results, invalid_columns

        except Exception as e:
            # Print error message if an exception occurs
            print(f"An error occurred: {str(e)}")
            # Return None for DataFrame and invalid columns
            return None, invalid_columns

    def get_table_info(self, table_name):
        """
        Retrieve information about a table from the database using SQLAlchemy ORM.

        Parameters:
            - table_name: str
                Name of the table in the database to retrieve information about.

        Returns:
            - pandas.DataFrame or None
                Returns a pandas DataFrame containing information about the specified table, including:
                    - Foreign key references
                    - Indexes
                    - Column details such as name, type, primary key status, and nullable status
                Returns None if the specified table does not exist in the database.

        Note:
            - The table_name parameter should match the name of the table in the database, not necessarily the ORM model name.
        """
        # Create a new MetaData object and bind it to the engine
        metadata = MetaData()
        metadata.reflect(bind=self.engine)

        # Get the specified table object
        table = metadata.tables.get(table_name)
        if table is None or str(table) == '':
            print(f"Table {table_name} doesn't exist")
            return None

        # Prepare data dictionary
        table_info = {
            "Foreign Keys": [],
            "Indexes": [],
            "Column Names": [],
            "Column Types": [],
            "Primary Keys": [],
            "Nullable": []
        }

        # Collect foreign key references, indexes, and column details
        for column in table.columns:
            # Foreign keys
            foreign_keys = column.foreign_keys
            if foreign_keys:
                for fk in foreign_keys:
                    table_info["Foreign Keys"].append(fk.target_fullname)

            # Column details
            table_info["Column Names"].append(column.name)
            table_info["Column Types"].append(str(column.type))
            table_info["Primary Keys"].append(column.primary_key)
            table_info["Nullable"].append(column.nullable)

        # Collect indexes
        if table.indexes:
            table_info["Indexes"] = [index.name for index in table.indexes]

        # Pad lists with None values to ensure equal lengths
        max_length = max(len(table_info["Foreign Keys"]), len(table_info["Indexes"]), len(table_info["Column Names"]))
        for key in table_info:
            table_info[key] += [None] * (max_length - len(table_info[key]))

        # Return table information as DataFrame
        return pd.DataFrame(table_info)

    def run_select_query(self, sql_statement):
        """
        Executes a SELECT SQL query and returns the results as a DataFrame.

        Args:
            sql_statement (str): The SQL SELECT statement to be executed.

        Returns:
            pandas.DataFrame: A DataFrame containing the query results.

        Raises:
            ValueError: If the provided SQL statement is not a SELECT statement.
            Exception: For any other exceptions that occur during the execution of the query.
        """
        with self.engine.connect() as connection:
            try:
                if not sql_statement.strip().lower().startswith('select'):
                    raise ValueError("Only SELECT statements are allowed.")
                # Convert string to a SQLAlchemy text object
                stmt = text(sql_statement)

                # Execute the text object on the connection
                result = connection.execute(stmt)

                # Get column names
                column_names = result.keys()

                # Handle different ways to retrieve data
                if result.rowcount == 0:  # No rows returned
                    data = []
                elif result.returns_rows:  # Fetch all rows (default)
                    data = result.fetchall()

                # Convert to DataFrame
                df = pd.DataFrame(data, columns=column_names)
                return df

            except (ValueError, Exception, psycopg2.Error) as error:
                print("Error:", error)
                raise  # Re-raise the exception

    def check_table_exists(self, table_name):
        """
        Checks if a table exists in the database schema.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names(schema=self.SCHEMA)

    # todo: separate delete and trunc
    def delete_or_truncate_table(self, table_name, action="trunc"):
        """
        Deletes or truncates the specified table.

        :param table_name: The name of the table to delete or truncate.
        :param action: The action to perform ("del" for delete or "trunc" for truncate).
        """
        if not self.check_table_exists(table_name):
            print(f"Table {table_name} does not exist.")
            return

        metadata = MetaData()
        metadata.reflect(bind=self.engine, schema=self.SCHEMA)  # todo: explain line if you can
        table = Table(table_name, metadata, autoload_with=self.engine)

        try:
            if action == "del":
                table.drop(self.engine)
                print(f"Table {table_name} deleted successfully.")
            elif action == "trunc":
                with self.engine.begin() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {self.SCHEMA}.{table_name} RESTART IDENTITY CASCADE"))
                print(f"Table {table_name} truncated successfully.")
            else:
                print(f"Invalid action: {action}. Please specify 'del' or 'trunc'.")
        except Exception as e:
            print(f"An error occurred while performing the action {action} on the table {table_name}: {str(e)}")

    def update_table(self, table_name, content, headers):
        pass

    def run_query(self):
        # self.engine.
        pass

    def filter_query_table(self, table_class, filter_column, filter_values, distinct=False, to_df=False):
        # table_class.metadata.schema = self.SCHEMA
        with self.get_db() as session:
            filter_list = [col.in_(vals) for col, vals in zip(filter_column, filter_values)]
            print(filter_list)
            query = session.query(table_class).filter(*filter_list)
            if distinct:
                query = query.distinct()

            if to_df:
                df = pd.read_sql(query.statement, self.engine)
                return df
            else:
                results = query.all()
                return results

    def compare_schema(self):
        """
        Compares the database schema as defined by SQLAlchemy models with the actual database schema.
                                                     1. todo:  to - by
        This function inspects the database connected to by the current DbService instance,
        compares the tables and columns defined in the SQLAlchemy models with those in the actual database,
        and identifies any discrepancies. Discrepancies can include missing tables, missing columns, extra columns,
        and type mismatches between the model definitions and the actual database schema.

        Returns:
            discrepancies (dict): A dictionary detailing any discrepancies found.
        """

        # Create an inspector to get metadata about the database
        inspector = inspect(self.engine)
        # Get a list of table names in the actual database schema
        actual_tables = inspector.get_table_names(schema=self.SCHEMA)
        # Initialize a dictionary to hold any discrepancies found
        discrepancies = {}

        # Iterate over all the model classes defined as subclasses of the Base class
        for table_class in Base.__subclasses__():
            # Get the table name from the model class
            table_name = table_class.__tablename__

            # Check if the table exists in the actual database
            if table_name not in actual_tables:
                # If the table is missing in the database, note it in discrepancies
                discrepancies[table_name] = 'Table missing in database'
                continue
            # todo: 2. rename the postgress db columns and sqlalchemy model columns
            # Get a dictionary of columns in the actual database table
            actual_columns = {col['name']: col for col in inspector.get_columns(table_name, schema=self.SCHEMA)}
            # Get a dictionary of columns defined in the SQLAlchemy model
            model_columns = {col.name: col for col in table_class.__table__.columns}

            # Determine which columns are missing in the database compared to the model
            missing_in_db = set(model_columns.keys()) - set(actual_columns.keys())
            # Determine which columns are extra in the database compared to the model
            extra_in_db = set(actual_columns.keys()) - set(model_columns.keys())
            # Identify type mismatches between the model and the actual database columns
            type_mismatches = {
                col: (str(model_columns[col].type), str(actual_columns[col]['type']))
                for col in model_columns if
                col in actual_columns and str(model_columns[col].type) != str(actual_columns[col]['type'])
            }

            # Record any discrepancies found for the table
            # todo:3. handle empty values: a. if one any of them empty, b. if all emtpy then should be handle differently
            discrepancies[table_name] = {
                'missing_in_db': list(missing_in_db),
                'extra_in_db': list(extra_in_db),
                'type_mismatches': type_mismatches
            }

        # Return the dictionary of discrepancies
        return discrepancies

    def get_schema_comparison(self):
        """
        Calls the compare_schema method and converts the result to a pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the schema discrepancies.
        """
        # Get discrepancies using compare_schema method
        discrepancies = self.compare_schema()

        # Prepare data for DataFrame
        discrepancies_list = []

        for table, issues in discrepancies.items():
            if isinstance(issues, str):  # Handle missing table case
                discrepancies_list.append(
                    {'table': table, 'column': None, 'issue': issues, 'model_type': None, 'db_type': None})
            else:  # Handle column discrepancies
                for issue_type, columns in issues.items():
                    if issue_type == 'type_mismatches':
                        for column, types in columns.items():
                            discrepancies_list.append({
                                'table': table,
                                'column': column,
                                'issue': issue_type,
                                'model_type': types[0],
                                'db_type': types[1]
                            })
                    else:
                        for column in columns:
                            discrepancies_list.append({
                                'table': table,
                                'column': column,
                                'issue': issue_type,
                                'model_type': None,
                                'db_type': None
                            })

        # Create DataFrame from the list
        df_discrepancies = pd.DataFrame(discrepancies_list)

        return df_discrepancies

# if __name__ == "__main__":
# from db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory,Volunteer, Base

# is_test=False
# ob = DbService(is_test)

# df = ob.get_schema_comparison()
# print(df)

# df, cols = ob.query_table_orm(table_name=Post, limit=15000,to_df=True, sort_by= {"publish_date": False})
# print(df)
#     # df = ob.get_table_info('test_table')
#
#     # print(df)
#
#     # ob.delete_or_truncate_table('test_table', 'del')
#
#     # headers = [
#     #     {"volunteer_id": "value10", "first_name": "value20", "last_name": "value30", "url": "value40"},
#     #     {"volunteer_id": "value50", "first_name": "value60", "last_name": "value70", "url": "value80"}
#     #     # Add more rows as needed
#     # ]
#
#     # # ob.insert_table_test(Volunteer,headers=headers)
#
#
#     # existing_creators = ob.filter_query_table(Creatort, [Creatort.name, Creatort.creator_image], [['Israel','gomaa1130'], ['TWITTER', 'TIKTOK']],to_df=True)
#     # ob.create_table(Post)
#     # self.db_obj.filter_query_table(Creatort, Creatort.name, self.df_data.name, True)


    # existing_creators = ob.filter_query_table(Creatort, [Creatort.name, Creatort.creator_image], [['Israel','gomaa1130'], ['TWITTER', 'TIKTOK']],to_df=True)
    # ob.create_table(Post)
    # self.db_obj.filter_query_table(Creatort, Creatort.name, self.data_df.name, True)