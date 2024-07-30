from uuid import uuid4

from nordigen import NordigenClient

# initialize Nordigen client and pass SECRET_ID and SECRET_KEY
client = NordigenClient(
    secret_id="7b62822d-f18e-453a-bc96-ee20d9de1f48",
    secret_key="e2a8dbba8b6cb8b8e3e4bc7b640540a1c1f4d45bbc36fb8e3bcd0ab4e0151f21b893c29ff4850ff09c983ceb9e1927070deff919f7bf8d4d4e1a10df141de14e"
)

# Create new access and refresh token
# Parameters can be loaded from .env or passed as a string
# Note: access_token is automatically injected to other requests after you successfully obtain it
token_data = client.generate_token()

# Use existing token
client.token = "YOUR_TOKEN"

# Exchange refresh token for new access token
new_token = client.exchange_token(token_data["refresh"])

# Get institution id by bank name and country
institution_id = client.institution.get_institution_id_by_name(
    country="LV",
    institution="Revolut"
)

# Get all institution by providing country code in ISO 3166 format
institutions = client.institution.get_institutions("LV")

# Initialize bank session
init = client.initialize_session(
    # institution id
    institution_id=institution_id,
    # redirect url after successful authentication
    redirect_uri="https://gocardless.com",
    # additional layer of unique ID defined by you
    reference_id=str(uuid4())
)

# Get requisition_id and link to initiate authorization process with a bank
link = init.link # bank authorization link
requisition_id = init.requisition_id