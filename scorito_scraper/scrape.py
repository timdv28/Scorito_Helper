import requests

def login_to_gamecenter():
    # Create a session to persist cookies
    session = requests.Session()

    # Step 1: (Optional) Get the login page to grab CSRF tokens or cookies
    login_page_url = "https://idsrv.scorito.com/Account/Login?ReturnUrl=%2Fconnect%2Fauthorize%2Fcallback%3Fclient_id%3DScorito.Website.Client%26redirect_uri%3Dhttps%253A%252F%252Fwww.scorito.com%252Fsignincallback%26response_type%3Dcode%26scope%3DScorito.Games.Cycling.API.Read%2520Scorito.Games.Cycling.API.Write%2520Scorito.Games.Motorsports.API.Read%2520Scorito.Games.Motorsports.API.Write%2520Scorito.Games.Football.API.Read%2520Scorito.Games.Football.API.Write%2520Scorito.Games.Cycling.API.Read%2520Scorito.Games.Cycling.API.Write%2520Scorito.Games.Tennis.API.Read%2520Scorito.Games.Tennis.API.Write%2520Scorito.Games.Darts.API.Read%2520Scorito.Games.Darts.API.Write%2520Scorito.Platform.API.Read%2520Scorito.Platform.API.Write%2520User.API%2520Scorito.Ranking.API.Read%2520Scorito.Score.API.Read%2520Scorito.Score.API.Write%2520openid%2520profile%2520email%2520roles%26state%3Dafcda3823b2a42f193a1162fbd213551%26code_challenge%3DBMDq8UxgFfxWs98xNXc44TwepSqmpYhV7SLmTLen7qY%26code_challenge_method%3DS256%26response_mode%3Dquery%26lang%3Dnl-NL%26webstyle%3D1"
    r = session.get(login_page_url)
    print(r)
    # If the site uses a CSRF token, parse it from r.text using BeautifulSoup

    # Step 2: Prepare your login data
    payload = {
        "username_field_name": "timdv28@hotmail.com",
        "password_field_name": "Tstone28",
        # "csrf_token": parsed_token  # if required
    }

    # Step 3: Send the login request
    login_post_url = "https://www.example.com/login-post-endpoint"
    r = session.post(login_post_url, data=payload)

    # Step 4: Check if login worked
    if "Welcome" in r.text or r.status_code == 200:
        print("Login successful!")
    else:
        print("Login failed.")

    # Step 5: Access the protected page
    gamecenter_url = "https://www.example.com/gamecenter"
    gc_response = session.get(gamecenter_url)
    print(gc_response.text)  # HTML of the gamecenter page


login_to_gamecenter()

###############################

# prices_data = ["€ 500.000", "€ 500.000", "€ 500.000"]

# for price_string in prices_data:
#   # You might need to clean up the string (remove currency symbols, spaces, commas for decimal)
#   cleaned_price_string = price_string.replace('€', '').replace(' ', '').replace('.', '').replace(',', '.') # Adjust cleaning based on actual format
#   try:
#     price_value = float(cleaned_price_string)
#     print(f"Extracted price: {price_value}")
#     # You can now work with the numerical price_value in your script
#   except ValueError:
#     print(f"Could not convert price to number: {price_string}")