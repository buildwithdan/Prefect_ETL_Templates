from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import requests
import json
from prefect import task, flow
from prefect.deployments import Deployment
from prefect_sqlalchemy import SqlAlchemyConnector

# Define the API endpoint
url = 'http://api.weatherapi.com/v1/current.json'

# Define your API key
api_key = '5e7394f9a18b4e11af0200141241807'

@task
def get_data(url, api_key):
    params = {
        'key': api_key,
        'q': 'Maidstone'
    }

    # Make the GET request
    response = requests.get(url, params=params)
    data = response.json()
    data = data["current"]

    if response.status_code == 200:
        print("Data collected successfully")
    else:
        print(f'Error: {response.status_code}')
        print(response.text)

    return data

# Create SQLAlchemy base
Base = declarative_base()

# Define the Weather model
class Weather(Base):
    __tablename__ = 'weather2'
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

@flow
def insert_data():
    data = get_data(url=url, api_key=api_key)
    # print(data)
    
    engine = create_engine('sqlite:///weather.db')

    Base.metadata.create_all(engine)
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
    print("Data submitted to database correctly")

    session.close()

# # Create deployment
# Deployment(
#     flow=insert_data,
#     name="weather-data-collection",
#     schedule=IntervalSchedule(interval=timedelta(minutes=30)),
#     tags=["weather", "API"],
# )

if __name__ == "__main__":
    insert_data()