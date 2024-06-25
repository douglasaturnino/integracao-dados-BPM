import os
import pandas as pd
from datetime import datetime
from repository.BPMODSRepository import DBConnectionBPMODS
from repository.BPMDWRepository import DBConnectionBPMDW


class dadosBPM:
    def __init__(self) -> None:
        path_data = os.path.join(os.getcwd(), "data", "BPM.csv")
        self.tbLogBPM = pd.read_csv(path_data)
        self.add_date()
        self.create_tbLogBPM()

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.tbLogBPM["dtCarga"] = dtCarga

    def create_tbLogBPM(self):
        with DBConnectionBPMODS() as db:
            print("Iniciando a criação da tabela tbLogBPM.")
            print("---------------------------------------")
            db.create()

    def create_dbODS(self):
        with DBConnectionBPMODS() as db:
            # Inserindo registros na tabela tbLogBPM
            print("Iniciando a inserção de dados na tabela tbLogBPM.")
            print("-------------------------------------------------")
            db.insert(self.tbLogBPM)
            print(
                f"Carga Finalizada! {len(self.tbLogBPM)} registros inseridos na tbLogBPM\n"
            )

    def create_dbDW(self):
        with DBConnectionBPMODS() as db:
            dBPM = db.select()

        with DBConnectionBPMDW() as db:
            db.create()
            db.delete()
            print("Iniciando a inserção de dados na tabela dBPM.")
            print("---------------------------------------------")
            db.insert(dBPM)
            print(f"Carga Finalizada! {len(dBPM)} registros inseridos na dBPM\n")
