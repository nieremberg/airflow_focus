import pandas as pd
from sqlalchemy.types import JSON
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from airflow.sdk import Variable
from typing import Union, Dict, List


def get_engine():
    """Criação centralizada de conexão com o banco de dados

    Returns:
        obj: Engine para acesso ao banco de dados
    """
    db = {
        "host": Variable.get("FINOPS_DB_HOST"),
        "database": Variable.get("FINOPS_DB"),
        "user": Variable.get("FINOPS_USER"),
        "password": Variable.get("FINOPS_PASSWORD"),
    }

    engine = create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:5432/{db['database']}",
        poolclass=NullPool,
    )

    return engine


def get_processed_files(provider: str) -> set:
    """Função responsável por recuperar as chaves processadas da tabela
    tb_processed_files como um conjunto.

    Args:
        provider(str): Sigla implementada do provider para a consulta
    Raises:
        e: Exceções gerais durante o processo de consulta

    Returns:
        list: Lista de arquivos processados por provedor
    """
    engine = get_engine()
    table = "tb_processed_files"

    try:
        query = f"SELECT key FROM {table} WHERE provider = '{provider}'"
        df = pd.read_sql(query, con=engine)

        return df["key"].tolist()

    except Exception as e:
        print(f"Erro ao recuperar dados da tabela {table}: {e}")
        raise e
    finally:
        engine.dispose()


def upsert_table(
    engine,
    df: pd.DataFrame,
    table_name: str,
    conflict_column: str,
    chunk_size: int = 5000,
    dtype_map=None,
):
    """
    Realiza uma operação de upsert (inserir ou atualizar) em uma tabela
    do banco de dados.

    Args:
        engine: Objeto SQLAlchemy engine conectado ao banco de dados.
        df: DataFrame do pandas contendo os dados a serem inseridos ou
        atualizados.
        table_name: Nome da tabela de destino no banco de dados.
        conflict_column: Nome da coluna que será usada para detectar
        conflitos (para a cláusula ON CONFLICT).
        chunk_size: Tamanho dos lotes para inserir dados na tabela
        temporária (padrão: 5000).

    Returns:
        None.

    Raises:
        SQLAlchemyError: Se ocorrer algum erro durante a interação com
        o banco de dados.
        Exception: Se ocorrer algum outro erro inesperado.

    Usage:
        conflict_column = "x_hash"
        upsert_table(
            self.engine,
            df,
            table,
            conflict_column,
            chunk_size,
            dtype_map
        )
    """
    temp_table = f"temp_{table_name}"
    try:
        with engine.begin() as connection:

            print(f"Iniciando a carga na tabela temporia {temp_table}")

            df.head(0).to_sql(
                temp_table,
                con=connection,
                if_exists="replace",
                index=False,
                dtype=dtype_map,
            )

            df.to_sql(
                temp_table,
                con=connection,
                if_exists="append",
                index=False,
                chunksize=chunk_size,
                dtype=dtype_map,
            )
            print(f"Término da carga na tabela temporia {temp_table}")

            print(f"Iniciado o upsert na tabela: {table_name}")
            all_columns = list(df.columns)
            update_columns = [
                col for col in all_columns if col != conflict_column
            ]
            update_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in update_columns
            )
            columns_csv = ", ".join(all_columns)

            upsert_sql = f"""
                INSERT INTO {table_name} ({columns_csv})
                SELECT {columns_csv} FROM {temp_table}
                ON CONFLICT ({conflict_column}) DO UPDATE
                SET {update_clause};
            """
            connection.execute(text(upsert_sql))
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
            print(f"Finalizado o upsert na tabela {table_name}")

    except Exception as e:
        print(f"Erro ao realizar upsert na tabela {table_name}: {e}")
        raise


def clean_data(
    engine,
    table_name: str,
    where_conditions: Union[Dict[str, any], List[Dict[str, any]]]
):
    """
    Exclui registros de uma tabela com base em condições dinâmicas.

    Parâmetros:
        engine: Conexão com o banco de dados
        table_name: Nome da tabela alvo
        where_conditions: Dicionário (ou lista de dicionários) com nome da coluna e valor a filtrar
    """
    try:
        print("Iniciando da limpeza da tabela ", table_name)
        if isinstance(where_conditions, dict):
            where_conditions = [where_conditions]

        clauses = []
        parameters = {}

        for i, condition in enumerate(where_conditions):
            clause_parts = []
            for key, value in condition.items():
                param_key = f"{key}_{i}"
                clause_parts.append(f"{key} = :{param_key}")
                parameters[param_key] = value
            clause = " AND ".join(clause_parts)
            clauses.append(f"({clause})")

        where_clause = " OR ".join(clauses)

        delete_sql = text(f"DELETE FROM {table_name} WHERE {where_clause}")

        with engine.begin() as connection:
            result = connection.execute(delete_sql, parameters)
            deleted = result.rowcount or 0
            print(
                f"Finalizado a limpeza da tabela {table_name}, {deleted} registros excluídos."
            )

    except Exception as e:
        print(f"Erro na limpeza da tabela {table_name}: {str(e)}")
        raise
