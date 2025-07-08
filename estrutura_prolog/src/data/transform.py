import pandas as pd


def transform_users(data):
    df = pd.DataFrame(data)
    df["role_id"] = df["role"].apply(lambda x: x.get("id") if isinstance(x, dict) else None)
    df["role_name"] = df["role"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    return df.drop(columns=["role"], errors="ignore")


def transform_checklists(data):
    df = pd.json_normalize(data)
    cols_drop = ["id", "formItemsAnswers", "syncedAt"]
    df.drop(columns=cols_drop, errors="ignore", inplace=True)
    df.rename(columns={"vehicle.licensePlate": "Placa", "submittedAt": "DataHoraEnvio"}, inplace=True)
    return df


def transform_vehicles(data):
    df = pd.json_normalize(data)
    df.rename(columns={"id": "Veiculo_id", "licensePlate": "Placa"}, inplace=True)
    return df


def transform_os(data):
    return pd.json_normalize(data)