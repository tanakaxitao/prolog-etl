from data.extract import extract_users, extract_checklists, extract_vehicles, extract_os
from data.transform import transform_users, transform_checklists, transform_vehicles, transform_os
from data.load import save_to_db, append_os_to_db
import time

# Função para rodar cada pipeline com intervalo de 5 minutos
def run_pipeline():
    # 🚧 Pipeline de usuários
    users = extract_users()
    save_to_db(transform_users(users), "usuarios_prolog")
    
    time.sleep(300)  # 5 minutos de intervalo

    # 🚧 Pipeline de veículos
    vehicles = extract_vehicles()
    save_to_db(transform_vehicles(vehicles), "veiculos_prolog")
    
    time.sleep(300)  # 5 minutos de intervalo

    # ✅ Pipeline de OS
    os = extract_os()
    append_os_to_db(transform_os(os), "ordens_servico_prolog")
    
    time.sleep(300)  # 5 minutos de intervalo

    # ✅ Pipeline de checklists
    checklists = extract_checklists()
    save_to_db(transform_checklists(checklists), "checklists_prolog")

# Rodar tudo
run_pipeline()
