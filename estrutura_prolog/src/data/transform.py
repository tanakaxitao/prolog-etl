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


def transform_os(order_details):
    """
    Transforma os dados brutos das Ordens de Serviço em formato achatado (flat).
    """
    flat_data = []

    for order in order_details:
        flat_order = order.copy()

        # Flatten completionBy
        if isinstance(order.get("completionBy"), dict):
            cb = order["completionBy"]
            flat_order["completionBy.id"] = cb.get("id")
            flat_order["completionBy.name"] = cb.get("name")
            flat_order["completionBy.serialNumber"] = cb.get("serialNumber")
            del flat_order["completionBy"]

        # Flatten createdBy
        if isinstance(order.get("createdBy"), dict):
            cr = order["createdBy"]
            flat_order["createdBy.id"] = cr.get("id")
            flat_order["createdBy.name"] = cr.get("name")
            flat_order["createdBy.serialNumber"] = cr.get("serialNumber")
            del flat_order["createdBy"]

        # Flatten vehicle
        if isinstance(order.get("vehicle"), dict):
            v = order["vehicle"]
            flat_order["vehicle.id"] = v.get("id")
            flat_order["vehicle.licensePlate"] = v.get("licensePlate")
            flat_order["vehicle.fleetId"] = v.get("fleetId")
            del flat_order["vehicle"]

        # Flatten workOrderItems
        if isinstance(order.get("workOrderItems"), list):
            for i, item in enumerate(order["workOrderItems"]):
                flat_order[f"workOrderItems_{i}_itemId"] = item.get("itemId")
                flat_order[f"workOrderItems_{i}_itemName"] = item.get("itemName")
                flat_order[f"workOrderItems_{i}_itemDescription"] = item.get("itemDescription")
                flat_order[f"workOrderItems_{i}_priority"] = item.get("priority")
                flat_order[f"workOrderItems_{i}_url"] = item.get("url")
            del flat_order["workOrderItems"]

        # Flatten resolutionAttachments
        if isinstance(order.get("resolutionAttachments"), list):
            for i, attachment in enumerate(order["resolutionAttachments"]):
                flat_order[f"resolutionAttachments_{i}_url"] = attachment.get("url")
            del flat_order["resolutionAttachments"]

        # Se já vier um número, usa direto. Se for lista, faz len()
        services = order.get("itemServices", [])
        products = order.get("itemProducts", [])
        flat_order["itemServices"] = services if isinstance(services, int) else len(services)
        flat_order["itemProducts"] = products if isinstance(products, int) else len(products)

        flat_data.append(flat_order)

    print(f"🔹 Dados transformados: {len(flat_data)} registros")
    return pd.DataFrame(flat_data)
