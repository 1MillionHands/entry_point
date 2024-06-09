from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData, asc, desc, Column, Integer, String, ForeignKey, inspect, Table
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import InvalidRequestError
import db_table_objects as tbls
from datetime import datetime
import json
import pandas as pd
import psycopg2
import re
import uuid


with open('./config_file.json', 'r') as f:
  config_data = json.load(f)
# with open(r'C:\Users\yanir\PycharmProjects\oneMilion\entry_point\DB_Manager_EP\config_file_.json', 'r') as f:
#   config_data = json.load(f)


class DbService:
    def __init__(self, is_test = True):
        self.engine = self.create_local_engine(test=is_test)
        self.SCHEMA = "omh_schema_test" if is_test else "omh_schema"
        # Create a session object to interact with the database
        self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

    @staticmethod
    def create_local_engine(test=False) -> object:
        HOST = config_data['aws_db']['host']
        PORT = config_data['aws_db']['port']
        DBNAME = config_data['aws_db']['dbname']
        SCHEMA = "omh_schema_test" if test else "omh_schema"
        USER = config_data['aws_db']['user']
        PASSWORD = config_data['aws_db']['password']

        # Define the database connection URL
        DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?options=-csearch_path%3D{SCHEMA}'
        print("Connecting to DB with URL:", DATABASE_URL)
        # Create a SQLAlchemy engine
        engine = create_engine(DATABASE_URL)


    @contextmanager
    def get_db(self):
        session = self.Session()
        try:
            # session.execute(text(f'SET search_path TO {self.SCHEMA}'))
            yield session
            session.commit()
        finally:
            session.close()

    def create_table(self, tbl_obj ):
        import logging

        # logging.basicConfig(level=logging.INFO)
        # logger = logging.getLogger('sqlalchemy.engine')
        # logger.setLevel(logging.INFO)

        if not self.engine.dialect.has_table(self.engine.connect(), tbl_obj.__tablename__):
            tbl_obj.metadata.create_all(self.engine)
        else:
            print("Table name already exists in the db")

    def create_schema(self, db, schema_name ):
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


    def query_table_orm(self, table_name, columns=None, chunk_size=10000, limit=None, sort_by=None, filters=None, distinct=False, to_df=False):
        # table_name.metadata.schema = self.SCHEMA
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
                    filter_list = []
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
                    query = query.filter(*filter_list)

                # Apply sorting if specified
                if sort_by is not None:
                    for col, ascending in sort_by.items():
                        if not hasattr(table_name, col):
                            invalid_columns.append(col)
                            continue

                        column_attr = getattr(table_name, col)
                        if ascending:
                            query = query.order_by(column_attr.asc())
                        else:
                            query = query.order_by(column_attr.desc())

                # Apply distinct if specified
                if distinct:
                    query = query.distinct()

                # Initialize offset for pagination
                offset = 0
                # List to store DataFrames fetched in chunks
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

    def delete_table(self, table_name, content, headers):
        pass

    def update_table(self, table_name, content, headers):
        pass

    def run_query(self):
        # self.engine.
        pass

    def filter_query_table(self, table_class, filter_column, filter_values, distinct = False, to_df = False):
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



# if __name__ == "__main__":
    # from db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory,Volunteer
    #
    # istest = False
    # ob  = DbService( istest)
    # filters = [
    # {
    #     "column": "platform_type",
    #     "values": ['TWITTER','INSTAGRAM'],
    # }]
    #
    # df, cols = ob.query_table_orm(table_name=Creatort,to_df=True)
    # # existing_creators = ob.filter_query_table(Creatort,\
    # #                                           [Post.creator_id],\
    # #                                           [['8c628039-dd8c-4395-b125-6708803dfd37']]\
    # #                                           ,to_df=True
    # #                                           )
    #
    # print(df)


    # headers = [
    #     {"volunteer_id": "value10", "first_name": "value20", "last_name": "value30", "url": "value40"},
    #     {"volunteer_id": "value50", "first_name": "value60", "last_name": "value70", "url": "value80"}
    #     # Add more rows as needed
    # ]
    #
    # ob.insert_table_test(Volunteer,headers=headers)


    # existing_creators = ob.filter_query_table(Creatort, [Creatort.name, Creatort.creator_image], [['Israel','gomaa1130'], ['TWITTER', 'TIKTOK']],to_df=True)
    # ob.create_table(Post)
    # self.db_obj.filter_query_table(Creatort, Creatort.name, self.data_df.name, True)