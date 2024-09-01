from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, engine, Engine
from sqlalchemy.orm import sessionmaker, declarative_base
import requests
from prefect import task, flow
from prefect.blocks.system import Secret
import logging

# Increase logging level
logging.basicConfig(level=logging.DEBUG)

# Load secrets
db_host = Secret.load("db-host").get()
db_user = Secret.load("db-user").get()
db_password = Secret.load("db-password").get()
db_name = "PrivateDB"
db_port = "1433"

# Define the API endpoint
url = 'http://api.weatherapi.com/v1/current.json'
api_key = Secret.load("weather-api-key").get()

@task(log_prints=True)
def get_data(url, api_key):
    logging.info("Starting data retrieval task")
    params = {
        'key': api_key,
        'q': 'Maidstone'
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()["current"]
        logging.info("Data collected successfully")
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

    return data

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

@task(log_prints=True)
def get_engine_db(bulk: bool=True) -> Engine:
    logging.info("Creating database connection string")
    con_str = engine.URL.create(
        "mssql+pyodbc",
        username=db_user,
        password=db_password,
        host=db_host,
        database=db_name,
        port=db_port,
        query={
            "driver": 'ODBC Driver 17 for SQL Server',
            "LongAsMax": "Yes",
        }
    )
    logging.info("Database connection string created")
    return create_engine(con_str, fast_executemany=bulk, echo=True)

@task(log_prints=True)
def setup_db(engine):
    logging.info("Setting up the database")
    Base.metadata.create_all(engine)
    logging.info("Database setup completed")

@task(log_prints=True)
def insert_data(engine, data):
    logging.info("Inserting data into the database")
    Session = sessionmaker(bind=engine)
    session = Session()

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

    session.add(weather)
    session.commit()
    logging.info("Data submitted to database correctly")
    session.close()

@flow(name="Weather Data Flow")
def logic_flow():
    logging.info("Starting the weather data flow")
    engine = get_engine_db()
    setup_db(engine)
    data = get_data(url=url, api_key=api_key)
    insert_data(engine, data)
    logging.info("Weather data flow completed")

if __name__ == "__main__":
    logic_flow()