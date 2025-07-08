from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME


def save_to_db(df, table_name, schema="Prolog"):
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        df.to_sql(table_name, engine, if_exists="replace", index=False, schema=schema)
        print(f"✅ {table_name} salvo com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao salvar {table_name}: {e}")