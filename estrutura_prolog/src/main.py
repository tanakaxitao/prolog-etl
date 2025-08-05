from data.extract import extract_users, extract_checklists, extract_vehicles, extract_os
from data.transform import transform_users, transform_checklists, transform_vehicles, transform_os
from data.load import save_to_db, append_os_to_db
import time

# 🚧 Outros pipelines desativados temporariamente
users = extract_users()
save_to_db(transform_users(users), "usuarios_prolog")

vehicles = extract_vehicles()
save_to_db(transform_vehicles(vehicles), "veiculos_prolog")

# ✅ Teste da pipeline de OS
os = extract_os()
append_os_to_db(transform_os(os), "ordens_servico_prolog")

time.sleep(1800)

checklists = extract_checklists()
save_to_db(transform_checklists(checklists), "checklists_prolog")
