import requests
import pandas as pd
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

    if r.status_code != 200:
        raise Exception(f"Erro ao gerar token: {r.text}")

    return r.json()["access_token"]


def extrair_pequeno():
    token = gerar_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    script = """
    SELECT
        User.Username,
        User.ID AS UserId
    FROM User
    """

    body = {
        "Script": script,
        "Args": {
            "PageNumber": 1,
            "PageSize": 10
        }
    }

    r = requests.post(URL_QUERY, json=body, headers=headers)

    resposta = r.json()
    resultados = resposta.get("Result", {}).get("Results", [])

    linhas = [item.get("Row", {}) for item in resultados]
    df = pd.DataFrame(linhas)

    # salva CSV no GitHub runner
    nome_arquivo = "teste.csv"
    df.to_csv(nome_arquivo, index=False, sep=";")

    print("Arquivo gerado:", nome_arquivo)
    print(df.head())


if __name__ == "__main__":
    extrair_pequeno()
