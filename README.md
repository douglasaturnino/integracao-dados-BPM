<div align='center'>
<img src="https://github.com/douglasaturnino/integracao-dados-BPM/assets/95532957/8b170540-30bf-4a63-b250-773559c49ed2"  width=400px/>
</div>

Este projeto foi feito com base na playlists do Prof. Claudio Bonel.

# Criação de um Data Warehouse e Carga de Dados

### Coleta de Dados do Site do IBGE

Os dados dos municípios do Rio de Janeiro foram coletados a partir do site do IBGE, disponível em [IBGE - Códigos dos Municípios do RJ](https://www.ibge.gov.br/explica/codigos-dos-municipios.php#RJ). Os dados foram carregados em um DataFrame, renomeados, e preparados para carga no banco de dados.

### Criação dos Bancos de Dados

Foram criados dois bancos de dados: ODS (Operational Data Store) e DW (Data Warehouse). Os arquivos SQLite foram criados para armazenar os dados operacionais e dimensionais, respectivamente.

### Manipulação dos Bancos de Dados

Os dados dos municípios foram carregados na tabela `tbLogMunic` no banco ODS. Em seguida, apenas as colunas relevantes foram selecionadas e carregadas na tabela `dMunicipio` no banco DW.

### Carga dos Dados de Cadastro das Delegacias de Polícia

Os dados de cadastro das delegacias de polícia foram carregados a partir de um arquivo CSV (`DP.csv`) e inseridos na tabela `tbLogDP` no banco ODS. Da mesma forma, os dados dos responsáveis pelas delegacias foram carregados de um arquivo CSV (`ResponsavelDP.csv`) e inseridos na tabela `tbLogRespDP` no mesmo banco.

### Construção das Dimensões e da Tabela de Fatos no Data Warehouse

No DW, foram criadas dimensões (`dDP` e `dBPM`) e uma tabela de fatos (`fOcorrencias`). As dimensões foram populadas com dados das delegacias de polícia e dos batalhões de polícia militar, respectivamente. A tabela de fatos foi preenchida com dados de ocorrências policiais, incluindo referências às dimensões criadas e ao período de ocorrência.

### Validação e Conclusão

Após cada etapa de carga, foram realizadas validações para garantir a integridade dos dados inseridos nos bancos de dados. A carga de dados foi concluída com sucesso, proporcionando um ambiente estruturado para análise de dados operacionais e históricos da segurança pública no Rio de Janeiro.

### Instruções de Uso

Para usar esse projeto é necessario rodar somente o comando

```bash
python main.py
```

