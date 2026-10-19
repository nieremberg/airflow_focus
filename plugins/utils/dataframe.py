# utils/dataframe.py

import json
import pytz
import hashlib
import pandas as pd
from typing import List
from dateutil import parser


def parse_datetime_columns(df: pd.DataFrame, columns: list[str], default_tz="UTC") -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            def parse_dt(val):
                if pd.isna(val):
                    return pd.NaT
                try:
                # Remove 'GMT' if present and parse the datetime
                    val = val.replace('GMT', '').strip()
                    dt = parser.parse(val)
                    if dt.tzinfo is None:
                        return dt.replace(tzinfo=pytz.timezone(default_tz))
                    else:
                        return dt
                except Exception as e:
                    print(f"Erro ao parsear '{val}' na coluna '{col}': {e}")
                    return pd.NaT
            df[col] = df[col].apply(parse_dt)
    return df



#def parse_datetime_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
#    """
#    Converte colunas especificadas de uma DataFrame para o tipo datetime com timezone UTC.
#
#    Args:
#        df (pd.DataFrame): O DataFrame de entrada com possíveis colunas de data.
#        columns (List[str]): Lista de nomes de colunas que devem ser convertidas.
#
#    Returns:
#        pd.DataFrame: O DataFrame com as colunas convertidas para datetime (UTC), se existirem.
#    """
#    for col in columns:
#        if col in df.columns:
#            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
#    return df




def parse_json_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Converte colunas com strings JSON em objetos Python (dicionários).

    Args:
        df (pd.DataFrame): O DataFrame de entrada com possíveis colunas em formato JSON.
        columns (List[str]): Lista de nomes de colunas que devem ser convertidas.

    Returns:
        pd.DataFrame: O DataFrame com as colunas JSON convertidas para objetos Python.

    """
    def safe_json(x):
        if pd.isna(x) or x is None:
            return None
        if isinstance(x, str):
            stripped = x.strip()
            if stripped == "" or stripped == "--":
                return None
            try:
                return json.loads(x)
            except (json.JSONDecodeError, ValueError):
                return None
        return x

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(safe_json)
    return df

def parse_child_resource_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Substitui valores '--' por None (NULL) nas colunas relacionadas a recursos filhos.

    Args:
        df (pd.DataFrame): O DataFrame de entrada.
        columns (List[str]): Lista de colunas que devem ter '--' substituído por None.

    Returns:
        pd.DataFrame: O DataFrame com as substituições aplicadas.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == "--" else x)
    return df


def generate_row_hash(row: pd.Series, ignore_columns: list = None) -> str:
    """
    Gera hash SHA256 com base em todos os valores da linha 

    Args:
        pd (Series): row de dataframe, consumido via apply
        ignore_columns (list, opcional): lista de nomes de colunas a ignorar no hash
    Returns:
        str: hash da linha do dataframe
    
    Usage: 
        ignore_columns = ["tags"]
        
        df["x_hash"] = df.apply(
            lambda row: generate_row_hash(
                row, ignore_columns=ignore_columns
            ),
            axis=1,
        )
    """
    if ignore_columns:
        row = row.drop(labels=ignore_columns, errors='ignore')

    valores = [str(val) if pd.notnull(val) else 'NULL' for val in row.values]
    row_string = '|'.join(valores)
    return hashlib.sha256(row_string.encode("utf-8")).hexdigest()
