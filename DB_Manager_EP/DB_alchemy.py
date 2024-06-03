from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData, asc, desc, Column, Integer, String, ForeignKey, inspect, Table
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import InvalidRequestError
import table_objects as tbls
import json
import pandas as pd
import psycopg2
import re
import uuid



with open('config_file.json', 'r') as f:
  config_data = json.load(f)
# with open('DB_Manager_EP/config_file_.json', 'r') as f:
#   config_data = json.load(f)


def create_local_engine(test = False):
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

    return engine

def get_db(engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = SessionLocal()
    try:
      yield db
    finally:
      db.close()

class DbService:

    def __init__(self, engine):
        self.engine = engine
        # Create a session object to interact with the database
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        # self.session = Session()

    @contextmanager
    def get_db(self,):
        SessionLocal = self.Session()

        try:
            yield SessionLocal
            SessionLocal.commit()
        except Exception as e:
            SessionLocal.rollback()
            raise e
        finally:
            self.Session.close()

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
            session.bulk_insert_mappings(tbl_obj, headers)
    @staticmethod
    def insert_table(db, tbl_obj, headers):
        db.bulk_insert_mappings(tbl_obj, headers)
        db.commit()


    def apply_sorting(self, query, table_name, sort_by):
        """
        Apply sorting criteria to the SQLAlchemy query.

        Parameters:
            - query: SQLAlchemy query object
                Query object to which sorting criteria will be applied.
            - table_name: SQLAlchemy Table object
                Table object representing the table being queried.
            - sort_by: dict
                Dictionary specifying sorting criteria where keys are column names and values are boolean
                indicating whether to sort in ascending (True) or descending (False) order.

        Returns:
            - SQLAlchemy query object
                Query object with sorting criteria applied.
        """
        for col_name, asc_order in sort_by.items():
            if asc_order:
                query = query.order_by(asc(getattr(table_name, col_name)))
            else:
                query = query.order_by(desc(getattr(table_name, col_name)))
        return query
    

    def print_table_info(self, table_info, table_name):
        """
        Print the information about a table.

        Parameters:
            - table_info: dict
                The information about the table.
            - table_name: str
                The name of the table.
        """
        print(f"Data of {table_name} table:")
        print("\nForeign Keys are:")
        for fk in table_info["Foreign Keys"]:
            if fk is not None:
                print(fk)

        print("\nIndexes are:")
        if table_info["Indexes"]:
            for index in table_info["Indexes"]:
                if index is not None:
                    print(index)
        else:
            print("None")

        print("\nColumns Data:")
        for i in range(len(table_info["Column Names"])):
            if table_info["Column Names"][i] is not None:
                print("Column Name:", table_info["Column Names"][i])
                print("Column Type:", table_info["Column Types"][i])
                print("Primary Key:", table_info["Primary Keys"][i])
                print("Nullable:", table_info["Nullable"][i])
                print()


    def query_table_orm(self, table_name, columns=None, chunk_size=10000, limit=None, sort_by=None):
        """
        Query data from a table in the database using SQLAlchemy ORM.

        Parameters:
            - self: Object
                Instance of the class containing the SQLAlchemy session.
            - table_name: str
                Name of the table to query data from.
            - columns: list of str, optional
                List of column names to fetch. If None, fetch all columns (default is None).
            - chunk_size: int, optional
                Number of rows to fetch per iteration (default is 10,000).
            - limit: int, optional
                Maximum number of rows to fetch (default is None, meaning no limit).
            - sort_by: dict, optional
                Dictionary specifying sorting criteria where keys are column names and values are boolean
                indicating whether to sort in ascending (True) or descending (False) order.

        Returns:
            - pandas.DataFrame
                A DataFrame containing the queried data.
            - list of str
                List of invalid column names if any.

        Raises:
            - Exception
                Any exception that occurs during the query execution.
        """
        # Initialize offset for pagination
        offset = 0
        # List to store DataFrames fetched in chunks
        res_df = []
        # List to store invalid column names
        invalid_columns = []
        try:
            # Loop until all data is fetched
            while True:
                # Start building the query for the specified table using SQLAlchemy ORM
                if columns is None:
                    query = self.session.query(table_name)
                else:
                    # Check if all specified columns exist
                    invalid_columns = [col for col in columns if not hasattr(table_name, col)]
                    valid_columns = [col for col in columns if col not in invalid_columns]
                    if invalid_columns:
                        print(f"Invalid column(s): {', '.join(invalid_columns)}")
                    query = self.session.query(*[getattr(table_name, col) for col in valid_columns])
                # Apply sorting if specified
                if sort_by is not None:
                    query = self.apply_sorting(query, table_name, sort_by)
                # Fetch data using pagination (offset and limit)
                res = query.offset(offset).limit(min(chunk_size, limit - offset) if limit is not None else chunk_size).all()
                # If no data is fetched, exit the loop
                if not res:
                    break
                # Convert fetched rows to dictionary format
                if columns is None:
                    data = [vars(row) for row in res]
                else:
                    data = [row for row in res]
                # Convert dictionary data to DataFrame
                chunked_df = pd.DataFrame(data)
                # Append DataFrame to list
                res_df.append(chunked_df)
                # Update offset for next iteration
                offset += chunk_size
                # Check if limit is reached
                if limit is not None and offset >= limit:
                    break
        except Exception as e:
            # Print error message if an exception occurs
            print(f"An error occurred: {str(e)}")
            # Return None for DataFrame and invalid columns
            return None, invalid_columns

        # Concatenate all DataFrames in the list
        final_df = pd.concat(res_df, ignore_index=True)
        # final_df.drop('_sa_instance_state', axis=1,inplace=True)
        return final_df, invalid_columns




    def get_table_info(self, table_name, as_df=False):
        """
        Retrieve information about a table from the database using SQLAlchemy ORM.

        Parameters:
            - table_name: str
                Name of the table in the database to retrieve information about.
            - as_df: bool, optional (default=False)
                If True, returns the table information as a pandas DataFrame.
                If False, prints the information to the console.

        Returns:
            - dict, pandas.DataFrame or None
                If as_df is False, returns None.
                If as_df is True, returns a pandas DataFrame containing information about the specified table, including:
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

        # Return table information based on as_df parameter
        if as_df:
            return pd.DataFrame(table_info)
        else:
            self.print_table_info(table_info, table_name)


    def run_select_query(self, sql_statement, return_dataframe=False):
        """
        Executes a SELECT SQL query and either prints the results or returns them as a DataFrame.

        Args:
            sql_statement (str): The SQL SELECT statement to be executed.
            return_dataframe (bool): If True, the results are returned as a pandas DataFrame. 
                                    If False, the results are printed and returned as a tuple of column names and data.

        Returns:
            If return_dataframe is True:
                pandas.DataFrame: A DataFrame containing the query results.
            If return_dataframe is False:
                tuple: A tuple containing the column names (list) and data (list of tuples).

        Raises:
            ValueError: If the provided SQL statement is not a SELECT statement.
            Exception: For any other exceptions that occur during the execution of the query.
        """
        with self.engine.connect() as connection:
            if isinstance(sql_to_exec, str):
                sql_to_exec = text(sql_to_exec)
            result = connection.execute(sql_to_exec)
        return result


    def delete_table(self, table_name, content, headers):
        pass

    def update_table(self, table_name, content, headers):
        pass

    def run_query(self):
        # self.engine.
        pass

    def filter_query_table(self, table_class, filter_column, filter_values, distinct = False, to_df = False):
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



if __name__ == "__main__":
    from DB_Manager_EP.db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory

    eng = create_local_engine(test='dev')
    ob  = DbService(eng)
    # existing_creators = ob.filter_query_table(Creatort, [Creatort.name, Creatort.creator_image], [['Israel','gomaa1130'], ['TWITTER', 'TIKTOK']],to_df=True)
    # ob.create_table(Post)
    # self.db_obj.filter_query_table(Creatort, Creatort.name, self.data_df.name, True)


    # for row in result:
    #     print(row)




    # with DbService(eng) as db_service:
    #     existing_creators = db_service.filter_query_table(Creatort, Creatort.name, ['Israel','gomaa1130'],to_df=True)