import os
import pandas as pd
from datetime import datetime
from repository.responsavelODSRepository import DBConnectionResponsavelODS


class dadosResponsavel:
    def __init__(self) -> None:
        path_data = os.path.join(os.getcwd(), "data", "ResponsavelDP.csv")
        self.tbLogRespDP = pd.read_csv(path_data)
        self.add_date()

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.tbLogRespDP["dtCarga"] = dtCarga

    def create_dbODS(self):
        with DBConnectionResponsavelODS() as db:
            # Inserindo registros na tabela tbLogRespDP
            print("Iniciando a inserção de dados na tabela tbLogRespDP.")
            print("----------------------------------------------------")
            db.insert(self.tbLogRespDP)
            print(
                f"Carga Finalizada! {len(self.tbLogRespDP)} registros inseridos na tbLogRespDP"
            )
