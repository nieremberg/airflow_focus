import os
import pandas as pd
from sqlalchemy.types import JSON
from loaders.base_loader import BaseLoader
from utils.database import get_engine
from utils.dataframe import (
    parse_datetime_columns,
    parse_json_columns,
)


class OCILoader(BaseLoader):
    def __init__(self, provider, temp_dir):
        super().__init__(provider, temp_dir)
        self.engine = get_engine()

    def read_files(self):
        csv_files = []
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return csv_files

    def transform(self, file):

        print(f"Iniciada a transformação do arquivo")

        df = pd.read_csv(file)
        
        df.columns = df.columns.str.lower()
        
        df = parse_datetime_columns(
            df,
            [
                "billingperiodstart",
                "billingperiodend",
                "chargeperiodstart",
                "chargeperiodend",
            ],
            default_tz="UTC"
        )
        
        df = parse_json_columns(df, ["tags"])

        print(f"Término da transformação do arquivo")
        return df

    def load_to_db(self, df, file):
        table = "tb_oci_focus"
        chunk_size = 5000
        dtype_map = {"tags": JSON}

        try:
            print("Iniciada a carga do arquivo")
            with self.engine.begin() as connection:
                df.to_sql(
                    table,
                    con=connection,
                    if_exists="append",
                    index=False,
                    dtype=dtype_map,
                    chunksize=chunk_size,
                )
            print("Término da carga do arquivo")

        except Exception as e:
            print(f"Erro ao inserir dados na tabela {table}: {e}")
            raise e
        finally:
            self.engine.dispose()
