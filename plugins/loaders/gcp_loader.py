import os
import json
import pandas as pd
from sqlalchemy.types import JSON
from loaders.base_loader import BaseLoader
from utils.database import get_engine, clean_data
from utils.dataframe import parse_datetime_columns


class GCPLoader(BaseLoader):
    def __init__(self, provider, temp_dir):
        super().__init__(provider, temp_dir)
        self.engine = get_engine()

    def read_files(self):
        csv_files = []
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return sorted(csv_files)

    def transform(self, file):
        print("Iniciada a transformação do arquivo GCP")

        df = pd.read_csv(file, low_memory=False)

        # Normaliza nomes das colunas para snake_case (minúsculo)
        df.columns = df.columns.str.strip().str.lower()

        # Converte colunas de data para datetime com timezone UTC
        datetime_columns = [
            "billing_period_start",
            "billing_period_end",
            "charge_period_start",
            "charge_period_end",
            "x_export_time",
        ]
        df = parse_datetime_columns(df, datetime_columns, default_tz="UTC")

        # Converte colunas JSONB (arrays/objetos no CSV)
        json_columns = [
            "x_credits",
            "x_system_labels",
            "x_labels",
            "x_project_labels",
            "x_tags",
        ]
        for col in json_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_json_field)

        print("Término da transformação do arquivo GCP")
        return df

    @staticmethod
    def _parse_json_field(value):
        """Tenta parsear campo JSON, retorna default se falhar."""
        if pd.isna(value) or value == "":
            return None
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None

    def load_to_db(self, df, file):
        table = "tb_gcp_focus"
        chunk_size = 5000
        dtype_map = {
            "x_credits": JSON,
            "x_system_labels": JSON,
            "x_labels": JSON,
            "x_project_labels": JSON,
            "x_tags": JSON,
        }

        # Limpa dados do mesmo período antes de inserir (evita duplicidade)
        if "billing_period_start" in df.columns and not df.empty:
            date_start = pd.to_datetime(df.iloc[0]["billing_period_start"], utc=True)
            date_end = pd.to_datetime(df.iloc[0]["billing_period_end"], utc=True)

            clean_data(
                engine=self.engine,
                table_name=table,
                where_conditions={
                    "billing_period_start": date_start,
                    "billing_period_end": date_end,
                },
            )

        try:
            print("Iniciada a carga do arquivo GCP")
            with self.engine.begin() as connection:
                df.to_sql(
                    table,
                    con=connection,
                    if_exists="append",
                    index=False,
                    dtype=dtype_map,
                    chunksize=chunk_size,
                )
            print("Término da carga do arquivo GCP")

        except Exception as e:
            print(f"Erro ao inserir dados na tabela {table}: {e}")
            raise e
        finally:
            self.engine.dispose()
