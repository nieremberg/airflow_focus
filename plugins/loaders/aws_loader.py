import os
import re
import pandas as pd
from datetime import datetime
from sqlalchemy.types import JSON
from loaders.base_loader import BaseLoader
from utils.files import aws_sort_files
from utils.database import get_engine, clean_data
from utils.dataframe import (
    parse_datetime_columns,
    parse_json_columns,
)


class AWSLoader(BaseLoader):

    # Competência no caminho: .../billing_period=YYYY-MM/...
    _BILLING_PERIOD_RE = re.compile(r"billing_period=([^/]+)")
    # Timestamp do diretório do snapshot: 2026-08-16T09:55:39.924Z...
    _SNAPSHOT_TS_RE = re.compile(
        r"/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)[^/]*/"
    )

    def __init__(self, provider, temp_dir):
        super().__init__(provider, temp_dir)
        self.engine = get_engine()
        self._pending_file_extension = ""

    def load(self, file_extension, paths=[]):
        # Guarda a extensão para que read_files possa registrar os
        # snapshots obsoletos como processados. A carga em si permanece
        # inalterada em BaseLoader.load.
        self._pending_file_extension = file_extension
        super().load(file_extension=file_extension, paths=paths)

    def _extract_billing_period(self, file: str) -> str:
        """Extrai a competência (ex.: '2026-08'). Fallback: diretório-pai."""
        match = self._BILLING_PERIOD_RE.search(file)
        return match.group(1) if match else os.path.dirname(file)

    def _extract_snapshot_key(self, file: str) -> tuple:
        """
        Identifica o snapshot do arquivo e devolve uma chave ordenável
        (timestamp, diretorio_do_snapshot). O snapshot mais recente tem o
        maior timestamp. Independe da extensão do arquivo (.csv/.csv.gz),
        pois usa o diretório do snapshot.
        """
        match = self._SNAPSHOT_TS_RE.search(file)
        if match:
            try:
                ts = datetime.strptime(
                    match.group(1), "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            except ValueError:
                ts = datetime.min
        else:
            ts = datetime.min
        snapshot_dir = os.path.dirname(file)
        return (ts, snapshot_dir)

    def _select_latest_snapshot_files(self, files: list) -> tuple:
        """
        Recebe a lista JÁ ORDENADA por aws_sort_files. Agrupa por
        competência, elege o snapshot mais recente de cada uma e separa
        os arquivos em (to_process, obsolete).

        Cada snapshot é a competência COMPLETA; dois snapshots do mesmo
        período não somam. Por isso processamos apenas o mais recente.

        IMPORTANTE: preserva a ordem de entrada. Como 'files' já vem
        ordenado por (timestamp_snapshot, numero_chunk), os chunks do
        snapshot vencedor saem em to_process na ordem 00001, 00002, ... —
        requisito da clean_data, que só dispara no chunk 00001.
        """
        # 1ª passada: snapshot mais recente por competência.
        latest_per_period = {}
        for f in files:
            period = self._extract_billing_period(f)
            key = self._extract_snapshot_key(f)
            current = latest_per_period.get(period)
            if current is None or key > current:
                latest_per_period[period] = key

        # 2ª passada: filtra mantendo a ordem original de 'files'.
        to_process, obsolete = [], []
        for f in files:
            period = self._extract_billing_period(f)
            _, snapshot_dir = self._extract_snapshot_key(f)
            if snapshot_dir == latest_per_period[period][1]:
                to_process.append(f)
            else:
                obsolete.append(f)
        return to_process, obsolete

    def read_files(self):
        csv_files = []
        for root, _, files in os.walk(self.temp_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        # Ordem obrigatória: clean_data só dispara no chunk 00001, então
        # 00001 precisa vir antes de 00002, 00003... dentro do snapshot.
        csv_files = aws_sort_files(csv_files)

        to_process, obsolete = self._select_latest_snapshot_files(csv_files)

        # Snapshots obsoletos: competências completas substituídas por um
        # snapshot mais recente. Registramos como processados para não
        # rebaixá-los em execuções futuras, mas NÃO os carregamos.
        for f in obsolete:
            if not self.is_file_processed(f, self._pending_file_extension):
                self.register_processed_file(f, self._pending_file_extension)

        if obsolete:
            print(
                f"Snapshots obsoletos ignorados na carga e registrados como "
                f"processados: {len(obsolete)} arquivo(s). Arquivos a "
                f"processar (snapshot mais recente): {len(to_process)}."
            )

        return to_process

    def transform(self, file):
        print("Iniciada a transformação do arquivo")

        df = pd.read_csv(file, low_memory=False)

        df.columns = df.columns.str.lower()
        df = parse_json_columns(df, ["tags", "x_costcategories", "x_discounts"])
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

        #df["effectivecost"] = df["effectivecost"].clip(lower=0)
        #df["billedcost"] = df["billedcost"].clip(lower=0)

        print("Término da transformação do arquivo")
        return df

    def load_to_db(self, df, file):

        table = "tb_aws_focus"
        chunk_size = 5000
        dtype_map = {
            "tags": JSON,
            "x_costcategories": JSON,
            "x_discounts": JSON,
        }

        # estratégia temporária para a carga da aws
        if self.is_first_csv_chunk(file):
            
            date_start = pd.to_datetime(df.iloc[0]["billingperiodstart"], utc=True)
            date_end = pd.to_datetime(df.iloc[0]["billingperiodend"], utc=True)

            clean_data(
                engine=self.engine,
                table_name=table,
                where_conditions={
                    "billingperiodstart": date_start,
                    "billingperiodend": date_end,
                    }
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
