import requests
import os

URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

def gerar_token():
    body = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "all"
    }

    r = requests.post(URL_TOKEN, data=body)

    print("Status token:", r.status_code)
    print("Resposta token:", r.text[:200])

    if r.status_code != 200:
        raise Exception("Erro ao gerar token")

    return r.json()["access_token"]


def teste_query():
    token = gerar_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    script = """
    SELECT
        User.Username,
        User.ID
    FROM User
    """

    body = {
        "Script": script,
        "Args": {
            "PageNumber": 1,
            "PageSize": 5,
            "Caching": -1
        }
    }

    r = requests.post(URL_QUERY, json=body, headers=headers)

    print("Status query:", r.status_code)
    print("Resposta query:", r.text[:500])


if __name__ == "__main__":
    teste_query()
