from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import requests
from prefect import task, flow
from prefect.blocks.system import Secret
import pyodbc

# Define the API endpoint
api_url = 'http://api.weatherapi.com/v1/current.json'

# Define your API key
secret_block = Secret.load("weather-api-key")
api_key_raw = secret_block.get()

# db details
db_host = Secret.load("db-host")
db_password = Secret.load("db-password")
db_user = Secret.load("db-user")

# connection_string = (
#     "mssql+pyodbc:///?odbc_connect="
#     "Driver={ODBC Driver 17 for SQL Server};"
#     f"Server={db_host};"
#     "Database=PrivateDB;"
#     f"Uid={db_user};"
#     f"Pwd={db_password};"
#     "Encrypt=no;"
#     "TrustServerCertificate=no;"
#     "Connection Timeout=30;"
# )

# Define the connection parameters
server = "dnell.database.windows.net"
database = "PrivateDB"
username = "r00t"
password = "8pdB$d@T^qDHk5BEWVLwSQ$zNBy5Xq&S*eg6"

# Create the connection string
connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
    "&Connection Timeout=30"
    )


# Load the connection string from the DatabaseCredentials block
# database_block = DatabaseCredentials.load("db-private")
# db_block2 = SqlAlchemyConnector.load("dnell-privatedb")
# engine = database_block.get_engine()

# Create SQLAlchemy base
Base = declarative_base()

# Define the Weather model
class Weather(Base):
    __tablename__ = 'weather_data'
    __table_args__ = {'schema': 'weather'}
    last_updated_epoch = Column(Integer, primary_key=True)
    last_updated = Column(String)
    temp_c = Column(Float)
    temp_f = Column(Float)
    is_day = Column(Boolean)
    condition_text = Column(String)
    condition_icon = Column(String)
    condition_code = Column(Integer)
    wind_mph = Column(Float)
    wind_kph = Column(Float)
    wind_degree = Column(Integer)
    wind_dir = Column(String)
    pressure_mb = Column(Float)
    pressure_in = Column(Float)
    precip_mm = Column(Float)
    precip_in = Column(Float)
    humidity = Column(Integer)
    cloud = Column(Integer)
    feelslike_c = Column(Float)
    feelslike_f = Column(Float)
    windchill_c = Column(Float)
    windchill_f = Column(Float)
    heatindex_c = Column(Float)
    heatindex_f = Column(Float)
    dewpoint_c = Column(Float)
    dewpoint_f = Column(Float)
    vis_km = Column(Float)
    vis_miles = Column(Float)
    uv = Column(Float)
    gust_mph = Column(Float)
    gust_kph = Column(Float)

@task
def get_data(url, api_key):
    params = {'key': api_key, 'q': 'Maidstone'}
    response = requests.get(url, params=params)
    data = response.json().get("current", {})

    if response.status_code == 200:
        print("Data collected successfully")
    else:
        print(f'Error: {response.status_code}')
        print(response.text)

    return data

@flow
def insert_data():
    data = get_data(url=api_url, api_key=api_key_raw)

    # Get the SQLAlchemy engine using the connection URL
    engine = create_engine(connection_string)

    # Ensure the table structure is created within the specified schema
    Base.metadata.create_all(engine)
    
    # Create a new session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create a Weather instance with the data
    weather = Weather(
        last_updated_epoch=data['last_updated_epoch'],
        last_updated=data['last_updated'],
        temp_c=data['temp_c'],
        temp_f=data['temp_f'],
        is_day=data['is_day'],
        condition_text=data['condition']['text'],
        condition_icon=data['condition']['icon'],
        condition_code=data['condition']['code'],
        wind_mph=data['wind_mph'],
        wind_kph=data['wind_kph'],
        wind_degree=data['wind_degree'],
        wind_dir=data['wind_dir'],
        pressure_mb=data['pressure_mb'],
        pressure_in=data['pressure_in'],
        precip_mm=data['precip_mm'],
        precip_in=data['precip_in'],
        humidity=data['humidity'],
        cloud=data['cloud'],
        feelslike_c=data['feelslike_c'],
        feelslike_f=data['feelslike_f'],
        windchill_c=data['windchill_c'],
        windchill_f=data['windchill_f'],
        heatindex_c=data['heatindex_c'],
        heatindex_f=data['heatindex_f'],
        dewpoint_c=data['dewpoint_c'],
        dewpoint_f=data['dewpoint_f'],
        vis_km=data['vis_km'],
        vis_miles=data['vis_miles'],
        uv=data['uv'],
        gust_mph=data['gust_mph'],
        gust_kph=data['gust_kph']
    )

    # Add the Weather instance to the session and commit
    session.add(weather)
    session.commit()
    print("Data submitted to database correctly")

    # Close the session
    session.close()

if __name__ == "__main__":
    insert_data()
    # print(database_block)
    # print(db_block2)
    # print(engine)
    # print(db_host)
    # print(db_password)
    # print(connection_string)
    
