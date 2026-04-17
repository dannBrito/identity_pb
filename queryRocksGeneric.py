import requests
import pandas as pd
import time
import os

# ===== CONFIG =====
URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

#  Pegando do GitHub Secrets
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

print("CLIENT_ID carregado?", CLIENT_ID )
print("CLIENT_SECRET carregado?", CLIENT_SECRET)

#  caminho agora é relativo (funciona no GitHub)
BASE_NOME_ARQUIVO = "baserole"

PAGE_SIZE = 100000
LIMITE_EXCEL = 1000000  # margem segura

# ===== TOKEN =====
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

    token = r.json()["access_token"]

    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

# ===== EXTRAÇÃO =====
def extrair_usuarios():

    headers = gerar_token()
    pagina = 1

    parte = 1
    linhas_na_parte = 0
    primeiro_bloco = True

    total_geral = 0

    while True:

        print(f"\n📄 Página {pagina}")

        script = """
        SELECT
            User.Username,
            User.ID AS UserId,
            Role.Name AS RoleName,
            Role.ID AS RoleId
        FROM
            RoleMember
        INNER JOIN User
            ON User.ID = split_part(RoleMember.ID, '_', 1)
        INNER JOIN Role
            ON Role.ID = regexp_replace(RoleMember.ID, '^[^_]+_', '')
        ORDER BY
            User.Username,
            Role.Name;
        """

        body = {
            "Script": script,
            "Args": {
                "PageNumber": pagina,
                "PageSize": PAGE_SIZE,
                "Caching": -1
            }
        }

        r = requests.post(URL_QUERY, json=body, headers=headers)

        #  Token expirado
        if r.status_code == 401:
            print(" Token expirado, renovando...")
            headers = gerar_token()
            continue

        if r.status_code != 200:
            print(" Erro HTTP:", r.status_code)
            print(r.text)
            break

        resposta = r.json()

        if not resposta.get("success"):
            print(" Erro na query:")
            print(resposta)
            break

        resultados = resposta.get("Result", {}).get("Results", [])
        qtd = len(resultados)

        print(f"   ➜ Registros recebidos: {qtd}")

        if qtd == 0:
            print(" Fim da extração.")
            break

        # Converter para DataFrame
        linhas = [item.get("Row", {}) for item in resultados]
        df = pd.DataFrame(linhas)

        #  troca de arquivo se necessário
        if linhas_na_parte >= LIMITE_EXCEL:
            parte += 1
            linhas_na_parte = 0
            primeiro_bloco = True
            print(f" Novo arquivo: {BASE_NOME_ARQUIVO}_{parte}.csv")

        nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

        # Salvar CSV
        df.to_csv(
            nome_arquivo,
            mode="w" if primeiro_bloco else "a",
            index=False,
            header=primeiro_bloco,
            sep=";"
        )

        primeiro_bloco = False
        linhas_na_parte += qtd
        total_geral += qtd

        print(f"   Linhas no arquivo atual: {linhas_na_parte}")
        print(f"   Total acumulado: {total_geral}")

        # Última página
        if qtd < PAGE_SIZE:
            print(" Última página alcançada.")
            break

        pagina += 1
        time.sleep(0.2)

    print(f"\n Extração finalizada! Total de registros: {total_geral}")

# ===== EXECUÇÃO =====
if __name__ == "__main__":
    extrair_usuarios()
