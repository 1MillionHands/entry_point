from sqlalchemy import create_engine, MetaData, asc, desc, Column, Integer, String, ForeignKey, inspect, Table
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import InvalidRequestError
import table_objects as tbls
import uuid
import json
import pandas as pd
import re
import pandas as pd
from contextlib import contextmanager


with open('DB_Manager_EP/config_file.json', 'r') as f:
  config_data = json.load(f)
# with open('entry_point/DB_Manager_EP/config_file.json', 'r') as f:
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


    def query_table_orm(self, table_name, columns=None, chunk_size=10000, limit=None, sort=None):
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
        - sort: list of tuples, optional
            List of tuples specifying sorting criteria, each tuple containing:
                - Column name (str)
                - Sorting order ('asc' or 'desc')

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
        dfs = []
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
                    for col in columns:
                        if not hasattr(table_name, col):
                            invalid_columns.append(col)
                    valid_columns = [col for col in columns if col not in invalid_columns]
                    if invalid_columns:
                        print(f"Invalid column(s): {', '.join(invalid_columns)}")
                    query = self.session.query(*[getattr(table_name, col) for col in valid_columns])
                # Apply sorting if specified
                if sort is not None:
                    for sort_option in sort:
                        # Check sorting order and apply accordingly
                        if sort_option[1].lower() == 'asc':
                            query = query.order_by(asc(getattr(table_name, sort_option[0])))
                        elif sort_option[1].lower() == 'desc':
                            query = query.order_by(desc(getattr(table_name, sort_option[0])))
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
                df = pd.DataFrame(data)
                # Append DataFrame to list
                dfs.append(df)
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

        # Concatenate all DataFrames into one final DataFrame
        final_df = pd.concat(dfs, ignore_index=True)
        # Drop the "_sa_instance_state" column added by SQLAlchemy ORM
        if "_sa_instance_state" in final_df.columns:
            final_df.drop("_sa_instance_state", axis=1, inplace=True)
        # Return the final DataFrame containing all queried data and list of invalid columns
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
            "Columns": []
        }

        # Collect foreign key references
        for column in table.columns:
            foreign_keys = column.foreign_keys
            if foreign_keys:
                for fk in foreign_keys:
                    table_info["Foreign Keys"].append(fk.target_fullname)

        # Collect indexes
        if table.indexes:
            table_info["Indexes"] = [index.name for index in table.indexes]

        # Collect column details
        for column in table.columns:
            column_info = {
                "Name": column.name,
                "Type": str(column.type),
                "Primary Key": column.primary_key,
                "Nullable": column.nullable
            }
            table_info["Columns"].append(column_info)

        # Pad lists with None values to ensure equal lengths
        max_length = max(len(table_info["Foreign Keys"]), len(table_info["Indexes"]), len(table_info["Columns"]))
        for key in table_info:
            table_info[key] += [None] * (max_length - len(table_info[key]))

        # Return table information based on as_df parameter
        if as_df:
            return pd.DataFrame(table_info)
        else:
            # Print table information
            print(f"Data of {table_name} table:")
            print("\nForeign Keys are:")
            for fk in table_info["Foreign Keys"]:
                print(fk)
            print("\nIndexes are:")
            if table_info["Indexes"]:
                for index in table_info["Indexes"]:
                    print(index)
            else:
                print("None")
            print("\nColumns Data:")
            for column in table_info["Columns"]:
                print("Column Name:", column["Name"])
                print("Column Type:", column["Type"])
                print("Primary Key:", column["Primary Key"])
                print("Nullable:", column["Nullable"])
                print()



    def simple_dql(self, sql_statement):
        """
        Execute a simple Data Query Language (DQL) SQL statement against the database using SQLAlchemy.

        Parameters:
        - sql_statement: str
            The SQL statement to execute.

        Returns:
        - ResultProxy
            The result of executing the SQL statement against the database.

        Note:
        - This method is intended for executing simple SELECT statements.
        - It extracts the table name from the SQL statement, retrieves the schema from the engine URL,
          and replaces the original table name with the fully qualified table name before execution.
        """

        # Define the regular expression pattern to match the table name
        pattern = r'FROM\s+(\w+)\s*'

        # Search for the pattern in the SQL statement
        match = re.search(pattern, sql_statement, re.IGNORECASE)

        if match:
            # Extract the table name from the matched group
            table_name = match.group(1)
        else:
            return None

        # Getting schema from engine url
        url_string = str(self.engine.url)
        table_schema = re.search(r'search_path%3D(.*?)(%|$)', url_string).group(1)

        # Building table name
        quoted_table_name = f'{table_schema}."{table_name}"'
        sql_to_exec = sql_statement.replace(table_name,quoted_table_name)
        print(sql_to_exec)

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





def convert_names_in_list(list_, old_column_name, new_column_name):
    # Iterate through the list of dictionaries and update the key
    try:
        for item in list_:
            if old_column_name in item:
                item[new_column_name] = item.pop(old_column_name)
        return list_
    except:
        print("old column name might not be in the list")
        return list_


# def update_posts_with_creator_id(post_list, session):
#     names_to_check = [post_data['name'] for post_data in post_list]
#     # Query the database to get creator_id based on the 'name' column
#     existing_creators = session.query(Creator).filter(Creator.name.in_(names_to_check)).all()
#     # creators = session.query(Creator.name, Creator.creator_id).all()
#     for post_data in post_list:
#         try:
#             existing_post = next(post for post in existing_creators if post.name == post_data['name'])
#             # Handle the case where a matching post was found
#             post_data['creator_fk'] = existing_post.creator_id
#         except StopIteration:
#             # Handle the case where no matching post was found
#             print("No existing post found")
#             break
#     return post_list


# def append_posts_fk_into_existis_posts(session, post_data_list):
#     urls_to_check = [post_data['url'] for post_data in posts_data_list]
#     # Query existing posts based on multiple URLs
#     existing_posts = session.query(Posts).filter(Posts.url.in_(urls_to_check)).all()
#
#     for post_data in posts_data_list:
#         try:
#             # Get the existing post from the database
#             existing_post = next(post for post in existing_posts if post.url == post_data['url'])
#             # Add the post_fk field to the existing post
#             post_data['post_fk'] = existing_post.id
#         except StopIteration:
#             break
#
#     return post_data_list


def insert_posts_bulk_ep(session, posts_data_list):
    # todo: refactor the fun; split it; check its functionalities
    # It split the posts to post id and post history
    #

    posts_list_not_exist_in_db = []
    posts_list_exist_in_db = []
    # Extract all URLs from the posts_data_list
    urls_to_check = [post_data['url'] for post_data in posts_data_list]

    # Query existing posts based on multiple URLs
    existing_posts = session.query(tbls.Post).filter(tbls.Post.url.in_(urls_to_check)).all()
    existing_post_urls = {post.url for post in existing_posts}

    for post_data in posts_data_list:
        post_data['post_history_id'] = str(uuid.uuid4())
        if post_data['url'] not in existing_post_urls:
            post_data['post_id'] = str(uuid.uuid4())
            posts_list_not_exist_in_db.append(post_data)
        else:
            try:
                # Get the existing post from the database
                existing_post = next(post for post in existing_posts if post.url == post_data['url'])
                # Add the post_fk field to the existing post
                post_data['post_fk'] = existing_post.post_id
                posts_list_exist_in_db.append(post_data)
            except StopIteration:
                break

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
    existing_creators = ob.filter_query_table(Creatort, [Creatort.name, Creatort.creator_image], [['Israel','gomaa1130'], ['TWITTER', 'TIKTOK']],to_df=True)
    # ob.create_table(Post)
    # self.db_obj.filter_query_table(Creatort, Creatort.name, self.data_df.name, True)


    # for row in result:
    #     print(row)




    # with DbService(eng) as db_service:
    #     existing_creators = db_service.filter_query_table(Creatort, Creatort.name, ['Israel','gomaa1130'],to_df=True)