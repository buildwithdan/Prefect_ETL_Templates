from prefect.deployments import Deployment
from weather_flow import weather_flow

deployment = Deployment.build_from_flow(
    flow=weather_flow,
    name="weather-api-deployment",
    schedule={"type": "cron", "value": "0,30 * * * *"},
    parameters={"url": "http://api.weatherapi.com/v1/current.json", "api_key": "5e7394f9a18b4e11af0200141241807"},
    tags=["testing", "tutorial"],
    description="Fetch weather data every 30 minutes and store it in a SQLite database.",
)

if __name__ == "__main__":
    deployment.apply()