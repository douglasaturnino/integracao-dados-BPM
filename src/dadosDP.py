import os
import pandas as pd
from datetime import datetime
from repository.DPODSRepository import DBConnectionDPODS
from repository.DPDWRepository import DBConnectionDPDW


class dadosDP:
    def __init__(self) -> None:
        path_data = os.path.join(os.getcwd(), "data", "DP.csv")
        self.tbLogDP = pd.read_csv(path_data)
        self.add_date()

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.tbLogDP["dtCarga"] = dtCarga

    def create_dbODS(self):
        with DBConnectionDPODS() as db:
            # Inserindo registros na tabela tbLogDP
            print("Iniciando a inserção de dados na tabela tbLogDP.")
            print("------------------------------------------------")
            db.insert(self.tbLogDP)
            print(
                f"Carga Finalizada! {len(self.tbLogDP)} registros inseridos na tbLogDP"
            )

    def create_dbDW(self):
        with DBConnectionDPODS() as db:
            dDP = db.select()

        with DBConnectionDPDW() as db:
            db.delete()
            print("Iniciando a inserção de dados na tabela dDP.")
            print("------------------------------------------------")
            db.insert(dDP)
            print(f"Carga Finalizada! {len(dDP)} registros inseridos na dDP")
