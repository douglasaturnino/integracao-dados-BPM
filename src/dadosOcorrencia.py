import os
import pandas as pd
from datetime import datetime
from repository.ocorrenciasODSRepository import DBConnectionOcorrenciasODS
from repository.ocorrenciasDWRepository import DBConnectionOcorrenciasDW


class dadosOcorrencia:
    def __init__(self) -> None:
        path_data = os.path.join(os.getcwd(), "data", "Ocorrencia.csv")
        self.tbLogOcorrencia = pd.read_csv(path_data)
        self.add_date()
        self.create_tbLogOcorrencia()

    def create_tbLogOcorrencia(self):
        with DBConnectionOcorrenciasODS() as db:
            print("Iniciando a criação da tabela tbLogOcorrencia.")
            print("----------------------------------------------")
            db.create()

    def add_date(self) -> None:
        # Adiciona uma coluna de data e hora decarga
        dtCarga = datetime.today().strftime("%d/%m/%Y %H:%M")
        self.tbLogOcorrencia["dtCarga"] = dtCarga

    def create_dbODS(self):
        with DBConnectionOcorrenciasODS() as db:
            # Inserindo registros na tabela tbLogOcorrencia
            print("Iniciando a inserção de dados na tabela tbLogOcorrencia.")
            print("--------------------------------------------------------")
            db.insert(self.tbLogOcorrencia)
            print(
                f"Carga Finalizada! {len(self.tbLogOcorrencia)} registros inseridos na tbLogOcorrencia\n"
            )

    def create_dbDW(self):
        with DBConnectionOcorrenciasODS() as db:
            fOcorrencia = db.select()

        with DBConnectionOcorrenciasDW() as db:
            db.create()
            # Criar uma tabela temporária
            fOcorrencia.to_sql("tempOcorrencias", db.conexao, if_exists="replace")
            carga_fOcorrencias = db.select()
            print("Carregando dados na tebela fOcorrencias.")
            print("----------------------------------------")
            db.insert(carga_fOcorrencias)
            print(
                f"Fim da carga incremental de dados na fOcorrencias! {len(carga_fOcorrencias)} registros inseridos\n"
            )
            print("Atualizando retoativa dos dados na tabela fOcorrencias")
            print("----------------------------------------")
            atualiza_fOcorrencias = db.update()
            print(
                f"Fim da Atualizando retoativa dos dados na fOcorrencias! {len(atualiza_fOcorrencias)} registros atualizados\n"
            )
