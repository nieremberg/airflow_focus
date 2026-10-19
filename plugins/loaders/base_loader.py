import os
import pandas as pd
from typing import Self
from abc import ABC, abstractmethod
from utils.database import get_engine
from sqlalchemy import text

class BaseLoader(ABC):
    def __init__(self, provider, temp_dir):
        self.temp_dir = temp_dir
        self.provider = provider

    @staticmethod
    def is_first_csv_chunk(file_path: str) -> bool:
        """
        Retorna True somente quando o arquivo CSV representa o primeiro
        chunk (ex.: "...-00001.csv" ou "...-1.csv").
        """
        file_name = os.path.basename(file_path)
        stem, extension = os.path.splitext(file_name)

        if extension.lower() != ".csv":
            return False

        last_token = stem.split("-")[-1]
        return last_token.isdigit() and int(last_token) == 1

    def _build_processed_key(self, file: str, file_extension: str) -> str:
        """
        Monta a chave de dedup para bater com a chave do objeto no bucket.

        Providers com objeto compactado (ex.: AWS/OCI, cujo bucket contém
        arquivos ".csv.gz") processam o arquivo já descompactado (".csv"),
        portanto a extensão de compactação precisa ser reanexada para
        reproduzir exatamente a chave do objeto no bucket.

        Providers sem compactação (ex.: GCP, cujo bucket já contém ".csv")
        NÃO devem concatenar a extensão, pois isso geraria chaves duplicadas
        do tipo ".csv.csv" e causaria re-download a cada execução.

        A condição `not endswith` cobre ambos os casos com uma única regra:
        só concatena quando o arquivo ainda não termina com a extensão.

        Args:
            file: Caminho do arquivo (já descompactado) processado.
            file_extension: Extensão do objeto original no bucket.

        Returns:
            str: Chave relativa ao temp_dir, no formato do objeto no bucket.
        """
        relative_key = os.path.relpath(file, self.temp_dir)
        if file_extension and not relative_key.endswith(file_extension):
            relative_key += file_extension
        return relative_key

    def load(self, file_extension, paths=[]):
        files = self.read_files() or []
        if not files:
            print("Nenhum arquivo encontrado.")
            return
        
        print(
            f"Iniciando o processamento de {len(files)} arquivos encontrados."
        )
        failed_files = []

        for file in files:
            
            if self.is_file_processed(file, file_extension):
                print(
                    f"Arquivo encontrado já processado. Abortando operação. Arquivo: {file}"
                    )
                continue

            print(f"Processando arquivo: {file}")
            try:
                df = self.transform(file)

                if df.empty:
                    print(f"Arquivo ignorado (vazio): {file}")
                else:
                    self.load_to_db(df, file)
                    print(f"SUCESSO: Dados inseridos com sucesso do arquivo {file}")

                self.register_processed_file(file, file_extension)
                
            except Exception as e:
                print(f"Erro ao processar o arquivo {file}: {e}")
                failed_files.append(file)
                continue

        if failed_files:
            print("Arquivos com problemas durante o processamento:")
            for failed_file in failed_files:
                print(f"- {failed_file}")
            raise RuntimeError(
            f"Falha ao processar {len(failed_files)} de {len(files)} arquivos."
            )
        else:
            print("SUCESSO: Todos os arquivos foram processados.")

    @abstractmethod
    def read_files(self):
        pass

    @abstractmethod
    def transform(self, file):
        pass

    @abstractmethod
    def load_to_db(self, df, file):
        pass

    def register_processed_file(self: Self, file: str, file_extension: str) -> None:
        """
        Registra o arquivo processado na tabela tb_processed_files

        Args:
            file (_type_): arquivo processado
            provider (_type_): provedor do arquivo

        Raises:
            e: _description_
        """
        key = self._build_processed_key(file, file_extension)
        df = pd.DataFrame({
                "provider": [self.provider],
                "date": [pd.Timestamp.now(tz="UTC")],
                "key": [key]
                })
        
        engine = get_engine()
        table = "tb_processed_files"
        try:
            with engine.begin() as connection:
                df.to_sql(
                    table,
                    con=connection,
                    if_exists="append",
                    index=False,
                )
            print(f"Arquivo registrado como processado. key={key}")
        except Exception as e:
            print(f"Erro ao registrar o arquivo na tabela {table}: {e}")
            raise e
        finally:
            engine.dispose()

    def is_file_processed(self: Self, file: str, file_extension: str) -> bool:
        """
        Verifica se o arquivo já foi processado

        Args:
            file (str): Caminho do arquivo a ser verificado.
            file_extension (str): Extensão do arquivo

        Returns:
            bool: True se o arquivo já foi processado, False caso contrário

        Raises:
            Exception: Em caso de erro de execução da consulta SQL.
        """
        query = text("""
                    SELECT 1
                    FROM tb_processed_files
                    WHERE provider = :provider
                    AND key = :key
                    LIMIT 1;
                """)
        params = {
            "provider": self.provider,
            "key": self._build_processed_key(file, file_extension)
        }
        
        engine = get_engine()
        table = "tb_processed_files"
        try:
            with engine.begin() as connection:
                result = connection.execute(query, params).fetchone()
                return result is not None
        except Exception as e:
            print(f"Erro ao verificar o arquivo na tabela {table}: {e}")
            raise e
        finally:
            engine.dispose()
