from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME


def get_engine():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def save_to_db(df, table_name, schema="Prolog"):
    try:
        engine = get_engine()
        df.to_sql(table_name, engine, if_exists="replace", index=False, schema=schema)
        print(f"✅ {table_name} salvo com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao salvar {table_name}: {e}")


def append_os_to_db(df, table_name="ordens_servico_prolog", schema="Prolog"):
    try:
        # Remover a coluna problemática, se existir
        df.drop(columns=["currentWorkOrderFlowStatus"], errors="ignore", inplace=True)

        engine = get_engine()
        df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema)
        print(f"✅ {table_name} atualizado com novas OS!")
    except Exception as e:
        print(f"❌ Erro ao adicionar novas OS em {table_name}: {e}")
