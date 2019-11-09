import os
import re
import sqlalchemy
import pandas as pd

def get_create_table_string(tablename, connection):
	sql = """
	select * from sqlite_master where name = "{}" and type = "table"
	""".format(tablename) 
	result = connection.execute(sql)

	return result.fetchmany()[0][4]

def add_pk_to_create_table_string(create_table_string, colname):
	regex = "(\n.+{}[^,]+)(,)".format(colname)

	return re.sub(regex, "\\1 PRIMARY KEY,",  create_table_string)
	
def add_pk_to_sqlite_table(tablename, index_column, connection):
	cts = get_create_table_string(tablename, connection)
	cts = add_pk_to_create_table_string(cts, index_column)

	template = """
	BEGIN TRANSACTION;
		ALTER TABLE {tablename} RENAME TO {tablename}_old_;
		{cts};
		INSERT INTO {tablename} SELECT * FROM {tablename}_old_;
		DROP TABLE {tablename}_old_;
	COMMIT TRANSACTION;
	"""

	create_and_drop_sql = template.format(tablename = tablename, cts = cts)
	connection.executescript(create_and_drop_sql)

if os.path.exists("aeronautical_occurrences_database.db"):
	os.remove("aeronautical_occurrences_database.db")
engine = sqlalchemy.create_engine("sqlite:///aeronautical_occurrences_database.db", echo=False)

anv = pd.read_csv('../data/anv.csv', sep='~')
ftc = pd.read_csv('../data/ftc.csv', sep='~')
oco = pd.read_csv('../data/oco.csv', sep='~')
rec = pd.read_csv('../data/rec.csv', sep='~')

anv.to_sql('AeronavesEnvolvidas', con=engine, if_exists='replace', index=False)
ftc.to_sql('FatoresContribuintes', con=engine, if_exists='replace', index=False)
oco.to_sql('Ocorrencias', con=engine, if_exists='replace', index=False)
rec.to_sql('RecomendacoesSeguranca', con=engine, if_exists='replace', index=False)

connection = engine.raw_connection()
cursor = connection.cursor()

add_pk_to_sqlite_table("AeronavesEnvolvidas", "(codigo_ocorrencia, aeronave_matricula)", cursor)
add_pk_to_sqlite_table("FatoresContribuintes", "(codigo_ocorrencia, fator_nome)", cursor)
add_pk_to_sqlite_table("Ocorrencias", "codigo_ocorrencia", cursor)
add_pk_to_sqlite_table("RecomendacoesSeguranca", "(codigo_ocorrencia, recomendacao_numero)", cursor)

connection.close()
