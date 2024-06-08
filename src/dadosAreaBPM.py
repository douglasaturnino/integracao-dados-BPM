import os
import pandas as pd
from datetime import datetime
from repository.areaBPMODSRepository import DBConnectionAreaBPMODS


class dadosAreaBPM:
    def __init__(self) -> None:
        path_data = os.path.join(os.getcwd(), "data", "areaBPM.csv")
        self.tbLogAreaBPM = pd.read_csv(path_data)
        self.add_date()

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.tbLogAreaBPM["dtCarga"] = dtCarga

    def create_dbODS(self):
        with DBConnectionAreaBPMODS() as db:
            # Inserindo registros na tabela tbLogAreaBPM
            print("Iniciando a inserção de dados na tabela tbLogAreaBPM.")
            print("----------------------------------------------------")
            db.insert(self.tbLogAreaBPM)
            print(
                f"Carga Finalizada! {len(self.tbLogAreaBPM)} registros inseridos na tbLogAreaBPM"
            )
