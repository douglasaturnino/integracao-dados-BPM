from repository.periodoDWRepository import DBConnectionPeriodoDW


class dadosPeriodo:
    def create_dbDW(self):
        with DBConnectionPeriodoDW() as db:
            dPeriodo = db.select()
            db.delete()
            print("Iniciando a inserção de dados na tabela dPeriodo.")
            print("-------------------------------------------------")
            db.insert(dPeriodo)
            print(f"Carga Finalizada! {len(dPeriodo)} registros inseridos na dPeriodo")
