# Guia de Contribuição

Obrigado por considerar contribuir com o Airflow FOCUS Pipeline! Este documento fornece diretrizes para contribuir com o projeto.

## Código de Conduta

Ao participar deste projeto, você concorda em manter um ambiente respeitoso e colaborativo.

## Como Posso Contribuir?

### Reportando Bugs

Antes de criar um bug report, verifique se o problema já não foi reportado. Se você encontrar um bug:

1. **Use um título claro e descritivo**
2. **Descreva os passos exatos para reproduzir o problema**
3. **Forneça exemplos específicos** para demonstrar os passos
4. **Descreva o comportamento observado** e explique por que esse comportamento é problemático
5. **Explique qual comportamento você esperava**
6. **Inclua screenshots** se relevante
7. **Inclua informações do ambiente**:
   - Versão do Airflow
   - Versão do Python
   - Sistema operacional
   - Versões dos SDKs dos provedores

### Sugerindo Melhorias

Se você tem uma ideia para melhorar o projeto:

1. **Use um título claro e descritivo**
2. **Forneça uma descrição detalhada da melhoria sugerida**
3. **Explique por que essa melhoria seria útil**
4. **Liste alguns exemplos de como a feature seria usada**

### Pull Requests

1. Fork o repositório
2. Crie uma branch a partir de `main`:
   ```bash
   git checkout -b feature/minha-feature
   # ou
   git checkout -b fix/meu-bug-fix
   ```

3. Faça suas alterações seguindo o [Guia de Estilo](#guia-de-estilo)

4. Adicione testes se aplicável

5. Certifique-se de que os testes passam:
   ```bash
   pytest
   ```

6. Commit suas mudanças:
   ```bash
   git commit -am 'Adiciona nova feature X'
   ```

7. Push para sua branch:
   ```bash
   git push origin feature/minha-feature
   ```

8. Abra um Pull Request

## Guia de Estilo

### Código Python

Este projeto segue as convenções do Python Enhancement Proposals (PEP):

- **PEP 8** - Style Guide for Python Code
- **PEP 257** - Docstring Conventions

#### Formatação

```python
# Use 4 espaços para indentação (não tabs)
def minha_funcao(param1, param2):
    """
    Breve descrição da função.

    Args:
        param1: Descrição do param1
        param2: Descrição do param2

    Returns:
        Descrição do retorno
    """
    return resultado
```

#### Naming Conventions

- **Funções e variáveis**: `snake_case`
- **Classes**: `PascalCase`
- **Constantes**: `UPPER_CASE`
- **Módulos**: `lowercase`

```python
# Bom
def calcular_total(items):
    TAXA_IMPOSTO = 0.1
    total = sum(items)
    return total * (1 + TAXA_IMPOSTO)

class ProcessadorFocus:
    pass

# Evitar
def CalcularTotal(items):  # Não use PascalCase para funções
    taxaImposto = 0.1      # Não use camelCase para variáveis
    return total
```

#### Imports

```python
# Ordem dos imports:
# 1. Biblioteca padrão
# 2. Bibliotecas de terceiros
# 3. Módulos locais

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks import download_new_objects_task
from utils.files import clean_temp_dir
```

#### Docstrings

Use docstrings no formato Google:

```python
def processar_arquivo(file_path, provider, temp_dir="/tmp"):
    """
    Processa um arquivo de dados do provedor especificado.

    Esta função baixa, descompacta e carrega os dados no banco de dados.

    Args:
        file_path (str): Caminho completo do arquivo a processar
        provider (str): Nome do provedor (aws, oci, gcp, hpc_focus)
        temp_dir (str, optional): Diretório temporário. Defaults to "/tmp".

    Returns:
        dict: Dicionário com métricas do processamento:
            - rows_processed: Número de linhas processadas
            - duration_ms: Tempo de processamento em milissegundos
            - status: Status do processamento (success/error)

    Raises:
        FileNotFoundError: Se o arquivo não existir
        ValueError: Se o provider for inválido

    Example:
        >>> result = processar_arquivo("/tmp/data.csv", "aws")
        >>> print(result['rows_processed'])
        1000
    """
    pass
```

#### Logging

Use o módulo de logging estruturado do projeto:

```python
from utils.logging_config import get_logger

logger = get_logger(__name__)
logger.set_context(provider=provider, task_id=task_id)

# Bom
logger.info("Processando arquivo", file_path=file_path, size_mb=size)

# Evitar
print(f"Processando arquivo {file_path}")  # Não use print
```

### DAGs do Airflow

#### Estrutura

```python
"""
Descrição clara do que a DAG faz.

Pipeline ETL que:
1. Passo 1
2. Passo 2
3. Passo 3
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Imports locais
from tasks import task_function
from utils.dag_callbacks import create_default_args_with_callbacks

# Configuração
default_args = create_default_args_with_callbacks(
    owner="airflow",
    retries=5,
    retry_delay_minutes=2
)

# DAG
with DAG(
    dag_id="nome_da_dag",
    description="Descrição breve",
    tags=["tag1", "tag2"],
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 10 * * *",
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Tasks
    task1 = PythonOperator(...)
    task2 = PythonOperator(...)

    # Dependências
    task1 >> task2
```

#### Naming

- **dag_id**: Use snake_case descritivo
  - ✅ `aws_focus_pipeline`
  - ❌ `AWS-Focus`, `awsFocus`

- **task_id**: Use snake_case descritivo
  - ✅ `download_new_objects`
  - ❌ `download`, `task1`

### Testes

#### Estrutura de Testes

```python
import pytest
from unittest.mock import Mock, patch

def test_download_new_objects_success():
    """Testa download bem-sucedido de objetos."""
    # Arrange
    provider = "aws"
    bucket = "test-bucket"

    # Act
    result = download_new_objects(provider, bucket)

    # Assert
    assert result['status'] == 'success'
    assert result['files_downloaded'] > 0
```

#### Cobertura

- Tente manter a cobertura de testes acima de 80%
- Foque em testar a lógica de negócio
- Use mocks para dependências externas (AWS, OCI, etc.)

### Commits

#### Mensagens de Commit

Use o formato Conventional Commits:

```
<tipo>(<escopo>): <descrição>

[corpo opcional]

[rodapé opcional]
```

**Tipos:**
- `feat`: Nova feature
- `fix`: Correção de bug
- `docs`: Mudanças na documentação
- `style`: Formatação, ponto-e-vírgula faltando, etc
- `refactor`: Refatoração de código
- `test`: Adição ou correção de testes
- `chore`: Mudanças em build, CI, etc

**Exemplos:**

```bash
feat(aws): adiciona suporte para arquivos parquet
fix(oci): corrige erro ao processar arquivos grandes
docs(readme): atualiza instruções de instalação
refactor(loaders): simplifica lógica do loader factory
test(tasks): adiciona testes para list_processed_keys
chore(deps): atualiza boto3 para versão 1.28.0
```

## Adicionando um Novo Provedor

Se você quer adicionar suporte para um novo provedor de nuvem:

### 1. Criar o Loader

Crie `plugins/loaders/novo_provider_loader.py`:

```python
from loaders.base_loader import BaseLoader
from utils.logging_config import get_logger

class NovoProviderLoader(BaseLoader):
    """Loader para o Novo Provider."""

    def __init__(self):
        self.logger = get_logger(__name__)

    def load_data(self, file_path, connection_string):
        """Carrega dados do arquivo no banco de dados."""
        # Implementação específica
        pass
```

### 2. Criar o Storage

Crie `plugins/storages/novo_provider_storage.py`:

```python
from utils.logging_config import get_logger

class NovoProviderStorage:
    """Storage para o Novo Provider."""

    def __init__(self):
        self.logger = get_logger(__name__)

    def download_file(self, bucket, key, local_path):
        """Baixa arquivo do storage do provedor."""
        # Implementação específica
        pass

    def list_objects(self, bucket, prefix):
        """Lista objetos no bucket."""
        # Implementação específica
        pass
```

### 3. Atualizar os Factories

Em `plugins/loaders/loader_factory.py`:

```python
def get_loader(provider: str):
    loaders = {
        'aws': AwsLoader,
        'oci': OciLoader,
        'gcp': GcpLoader,
        'hpc_focus': HpcLoader,
        'novo_provider': NovoProviderLoader,  # Adicionar aqui
    }
    return loaders.get(provider)()
```

Em `plugins/storages/storage_factory.py`:

```python
def get_storage(provider: str):
    storages = {
        'aws': AwsStorage,
        'oci': OciStorage,
        'gcp': GcpStorage,
        'hpc_focus': HpcStorage,
        'novo_provider': NovoProviderStorage,  # Adicionar aqui
    }
    return storages.get(provider)()
```

### 4. Criar a DAG

Crie `dags/novo_provider_focus_pipeline_dag.py`:

```python
"""
DAG para carregar dados do Novo Provider para o Postgres.

Pipeline ETL que:
1. Lista arquivos já processados no banco
2. Baixa novos objetos do bucket do provedor na nuvem
3. Carrega dados no PostgreSQL
4. Sincroniza arquivos raw para Dell ECS (object storage on-premises)
5. Limpa diretório temporário
"""

from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator

from tasks import (
    list_processed_keys_task,
    download_new_objects_task,
    upload_to_dell_ecs_task,
    load_to_db_task,
)
from utils.files import clean_temp_dir
from utils.dag_callbacks import create_default_args_with_callbacks

default_args = create_default_args_with_callbacks(
    owner="airflow",
    retries=5,
    retry_delay_minutes=2
)

with DAG(
    dag_id="novo_provider_focus_pipeline",
    description="Carrega dados do Novo Provider para o banco de dados",
    tags=["novo_provider", "focus", "etl"],
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 10 * * *",
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    list_processed_keys = PythonOperator(
        task_id="list_processed_keys",
        python_callable=list_processed_keys_task,
        op_kwargs={"provider": "novo_provider"},
    )

    download_new_objects = PythonOperator(
        task_id="download_new_objects",
        python_callable=download_new_objects_task,
        op_kwargs={
            "provider": "novo_provider",
            "bucket": Variable.get("NOVO_PROVIDER_BUCKET"),
            "prefix": Variable.get("NOVO_PROVIDER_PREFIX"),
            "temp_dir": "/tmp/novo_provider_downloads",
            "list_processed_files": "{{ task_instance.xcom_pull(task_ids='list_processed_keys') }}",
        },
    )

    load_to_db = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db_task,
        op_kwargs={
            "provider": "novo_provider",
            "temp_dir": "/tmp/novo_provider_downloads",
            "file_extension": ".csv",
        },
    )

    upload_to_dell_ecs = PythonOperator(
        task_id="upload_to_dell_ecs",
        python_callable=upload_to_dell_ecs_task,
        op_kwargs={
            "provider": "ecs",
            "bucket": Variable.get("ECS_BUCKET"),
            "prefix": Variable.get("ECS_NOVO_PROVIDER_PREFIX"),
            "temp_dir": "/tmp/novo_provider_downloads",
            "file_extension": ".csv",
        },
    )

    clean_task = PythonOperator(
        task_id="clean_temp_dir",
        python_callable=clean_temp_dir,
        op_kwargs={"temp_dir": "/tmp/novo_provider_downloads"},
    )

    (
        list_processed_keys
        >> download_new_objects
        >> load_to_db
        >> upload_to_dell_ecs
        >> clean_task
    )
```

### 5. Atualizar Documentação

- Adicione o novo provedor ao README.md
- Atualize QUICKSTART.md com instruções de configuração
- Atualize CHANGELOG.md
- Adicione variáveis ao airflow_variables.example.json

### 6. Adicionar Testes

Crie testes para o novo loader e storage:

```python
# tests/test_novo_provider_loader.py
def test_novo_provider_loader():
    loader = NovoProviderLoader()
    # Testes...
```

## Checklist do Pull Request

Antes de submeter seu PR, verifique:

- [ ] O código segue o guia de estilo do projeto
- [ ] Docstrings foram adicionadas/atualizadas
- [ ] Testes foram adicionados/atualizados
- [ ] Todos os testes passam
- [ ] A documentação foi atualizada (README, QUICKSTART)
- [ ] CHANGELOG.md foi atualizado
- [ ] As mensagens de commit seguem o padrão
- [ ] Não há credenciais ou senhas no código

## Dúvidas?

Se você tiver dúvidas sobre como contribuir, abra uma issue com a tag `question`.

Obrigado pela sua contribuição! 🎉
