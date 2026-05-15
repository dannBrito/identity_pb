import glob
import os

from query_users import extrair_usuarios
from query_roles import extrair_roles

# ============================================
# LIMPA CSV ANTIGO
# ============================================
print(" Limpando arquivos antigos...", flush=True)

for f in glob.glob("baseusers_*.csv"):
    os.remove(f)

for f in glob.glob("baserole_*.csv"):
    os.remove(f)

# ============================================
# USERS
# ============================================
print("\n EXTRAINDO USERS", flush=True)

extrair_usuarios()

# ============================================
# ROLES
# ============================================
print("\n EXTRAINDO ROLES", flush=True)

extrair_roles()

print("\n PIPELINE FINALIZADO", flush=True)
