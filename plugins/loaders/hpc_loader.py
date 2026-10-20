import os
import pandas as pd
from sqlalchemy.types import JSON
from loaders.base_loader import BaseLoader
from utils.files import hpc_sort_files
from utils.database import get_engine, clean_data
from utils.dataframe import (
    parse_datetime_columns,
    parse_json_columns,
    parse_child_resource_columns,
)


class HPCLoader(BaseLoader):
    def __init__(self, provider, temp_dir):
        super().__init__(provider, temp_dir)
        self.engine = get_engine()

    def _build_processed_key(self, file: str, file_extension: str) -> str:
        """
        Chave de dedup baseada no diretório do snapshot, não no arquivo
        individual: um .zip da Huawei pode descompactar em múltiplos CSVs,
        e a chave precisa corresponder ao nome do .zip original no bucket
        (usado tanto por is_file_processed quanto por register_processed_file).
        """
        return os.path.relpath(os.path.dirname(file), self.temp_dir) + file_extension

    def read_files(self):
        csv_files = []
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return hpc_sort_files(csv_files)

    def transform(self, file):
        print("Iniciada a transformação do arquivo")

        try:
            df = pd.read_csv(file, low_memory=False)
        except pd.errors.EmptyDataError:
            print(f"Arquivo sem dados: {file}")
            return pd.DataFrame()

        if df.empty or df.dropna(how="all").empty:
            print(f"Arquivo sem linhas válidas: {file}")
            return pd.DataFrame()

        COLUMN_RENAMES = {
            "Month": "month",
            "Time Range": "time_range",
            "Linked Account": "linked_account",
            "PayerAccount Name": "payeraccount_name",
            "Business Entity": "business_entity",
            "Service Type Code": "service_type_code",
            "Service Type": "service_type",
            "Resource Type Code": "resource_type_code",
            "Resource Type": "resource_type",
            "Service Type Code (Child Resource)": "service_type_code_child_resource",
            "Service Type (Child Resource)": "service_type_child_resource",
            "Resource Type Code (Child Resource)": "resource_type_code_child_resource",
            "Resource Type (Child Resource)": "resource_type_child_resource",
            "Product ID": "product_id",
            "Specification Code": "specification_code",
            "Specifications": "specifications",
            "Region Code": "region_code",
            "Region": "region",
            "AZ": "az",
            "Billing Mode": "billing_mode",
            "Bill Type": "bill_type",
            "Order No.": "order_no",
            "Start Time": "start_time",
            "End Time": "end_time",
            "Usage Type Code": "usage_type_code",
            "Usage Type": "usage_type",
            "Usage Unit": "usage_unit",
            "Usage": "usage",
            "Package Usage": "package_usage",
            "Usage in Reserved Instances": "usage_in_reserved_instances",
            "List Price (USD)": "list_price_usd",
            "Original Cost (USD)": "original_cost_usd",
            "Coupons Used (USD)": "coupons_used_usd",
            "Spot": "spot",
            "Resource ID": "resource_id",
            "Resource Name": "resource_name",
            "Child Resource ID": "child_resource_id",
            "Child Resource Name": "child_resource_name",
            "Savings Plan/ID": "savings_plan_id",
            "Savings Plan/Payment Option": "savings_plan_payment_option",
            "Savings Plan/Discount": "savings_plan_discount",
            "Savings Plan/Hourly Commitment (USD)": "savings_plan_hourly_commitment_usd",
            "Savings Plan/Amount Deducted (USD)": "savings_plan_amount_deducted_usd",
            "Enterprise Project/ID": "enterprise_project_id",
            "Enterprise Project/Name": "enterprise_project_name",
            "Resource Tag": "resource_tag",
            "Cost Category": "cost_category",
        }

        df.rename(columns=COLUMN_RENAMES, inplace=True)
        
        df = parse_json_columns(df, ["resource_tag"])


        if "time_range" in df.columns:
            time_split = df["time_range"].str.split("/", expand=True)
            df["start_time_range"], df["end_time_range"] = time_split[0], time_split[1]

        df = parse_datetime_columns(
            df,
            ["start_time", "end_time", "start_time_range", "end_time_range"],
        )
        
        df = parse_child_resource_columns(
            df,
            [
                "service_type_code_child_resource",
                "service_type_child_resource",
                "resource_type_code_child_resource",
                "resource_type_child_resource",
                "child_resource_id",
                "child_resource_name",
            ],
        )

        print("Término da transformação do arquivo")
        return df

    def load_to_db(self, df, file):

        table = "tb_hpc_original_costs"
        chunk_size = 5000
        dtype_map = {
            "resource_tag": JSON,
        }

        # estratégia temporária para a carga da Huawei
        if file.endswith("1.csv"):
            invoice_month = df.iloc[0]["month"]
            clean_data(
                engine=self.engine,
                table_name=table,
                where_conditions={"month": invoice_month}
            )

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


class HPCFocusLoader(BaseLoader):
    def __init__(self, provider, temp_dir):
        super().__init__(provider, temp_dir)
        self.engine = get_engine()

    def _build_processed_key(self, file: str, file_extension: str) -> str:
        """
        Chave de dedup baseada no diretório do snapshot, não no arquivo
        individual: um .zip da Huawei pode descompactar em múltiplos CSVs,
        e a chave precisa corresponder ao nome do .zip original no bucket
        (usado tanto por is_file_processed quanto por register_processed_file).
        """
        return os.path.relpath(os.path.dirname(file), self.temp_dir) + file_extension

    def read_files(self):
        csv_files = []
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.lower().endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return hpc_sort_files(csv_files)

    def transform(self, file):
        print("Iniciada a transformação do arquivo")

        try:
            df = pd.read_csv(file, low_memory=False)
        except pd.errors.EmptyDataError:
            print(f"Arquivo sem dados: {file}")
            return pd.DataFrame()

        if df.empty or df.dropna(how="all").empty:
            print(f"Arquivo sem linhas válidas: {file}")
            return pd.DataFrame()

        df.columns = df.columns.str.lower()

        # Mapear nomes de colunas que mudaram nos CSVs da Huawei
        # para manter compatibilidade com o schema existente no banco
        COLUMN_RENAMES = {
            "invoiceissuername": "invoiceissuer",
            "providername": "provider",
            "publishername": "publisher",
        }
        df.rename(columns=COLUMN_RENAMES, inplace=True)

        datetime_cols = [
            "billingperiodstart",
            "billingperiodend",
            "chargeperiodstart",
            "chargeperiodend",
        ]
        existing_dt_cols = [c for c in datetime_cols if c in df.columns]

        if existing_dt_cols:
            df = parse_datetime_columns(df, existing_dt_cols)

        df = parse_json_columns(df, ["tags"])

        print("Término da transformação do arquivo")
        return df

    def load_to_db(self, df, file):
        table = "tb_hpc_focus"
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
