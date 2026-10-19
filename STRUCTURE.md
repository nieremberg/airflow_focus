# Estrutura do Repositório

```
airflow_focus/
│
├── dags/                                   # Apache Airflow DAGs
│   ├── aws_focus_pipeline_dag.py          # Pipeline FOCUS para AWS
│   ├── oci_focus_pipeline_dag.py          # Pipeline FOCUS para Oracle Cloud
│   ├── gcp_focus_pipeline_dag.py          # Pipeline FOCUS para Google Cloud
│   └── hpc_focus_pipeline_dag.py          # Pipeline FOCUS para Huawei Cloud
│
├── plugins/                                # Plugins e módulos do Airflow
│   ├── __init__.py
│   ├── tasks.py                           # Funções de tarefas compartilhadas
│   │
│   ├── loaders/                           # Loaders por provedor
│   │   ├── __init__.py
│   │   ├── base_loader.py                 # Classe base para loaders
│   │   ├── loader_factory.py              # Factory para criar loaders
│   │   ├── aws_loader.py                  # Loader específico AWS
│   │   ├── oci_loader.py                  # Loader específico OCI
│   │   ├── gcp_loader.py                  # Loader específico GCP
│   │   └── hpc_loader.py                  # Loader específico Huawei
│   │
│   ├── storages/                          # Integrações com storage
│   │   ├── __init__.py
│   │   ├── storage_factory.py             # Factory para criar storages
│   │   ├── aws_storage.py                 # Storage AWS S3
│   │   ├── oci_storage.py                 # Storage OCI Object Storage
│   │   ├── gcp_storage.py                 # Storage GCP Cloud Storage
│   │   ├── hpc_storage.py                 # Storage Huawei OBS
│   │   └── ecs_storage.py                 # Storage para backup (S3-compatible)
│   │
│   ├── utils/                             # Utilitários
│   │   ├── __init__.py
│   │   ├── files.py                       # Manipulação de arquivos (compress/decompress)
│   │   ├── database.py                    # Operações de banco de dados
│   │   ├── dataframe.py                   # Manipulação de DataFrames
│   │   ├── logging_config.py              # Configuração de logging estruturado
│   │   └── dag_callbacks.py               # Callbacks para DAGs (success/failure)
│   │
│   └── interfaces/                        # Interfaces e contratos
│       ├── __init__.py
│       └── storage_interface.py           # Interface para storages
│
├── requirements.txt                        # Dependências Python
├── .gitignore                             # Arquivos a ignorar no Git
│
├── README.md                              # Documentação principal
├── QUICKSTART.md                          # Guia de início rápido
├── CONTRIBUTING.md                        # Guia de contribuição
├── CHANGELOG.md                           # Histórico de mudanças
├── STRUCTURE.md                           # Este arquivo
├── LICENSE                                # Licença do projeto
│
└── airflow_variables.example.json        # Exemplo de variáveis do Airflow

```

## Descrição dos Componentes

### DAGs (`dags/`)

Contém as DAGs do Apache Airflow que orquestram o pipeline ETL para cada provedor:

- **aws_focus_pipeline_dag.py**: Coleta dados FOCUS da AWS (arquivos .gz)
- **oci_focus_pipeline_dag.py**: Coleta dados FOCUS da Oracle Cloud (arquivos .gz)
- **gcp_focus_pipeline_dag.py**: Coleta dados FOCUS do Google Cloud (arquivos .csv)
- **hpc_focus_pipeline_dag.py**: Coleta dados FOCUS da Huawei Cloud (arquivos .zip)

### Plugins (`plugins/`)

#### tasks.py
Funções de tarefas reutilizáveis que são chamadas pelas DAGs:
- `list_processed_keys_task()`: Lista arquivos já processados
- `download_new_objects_task()`: Baixa novos objetos do bucket
- `load_to_db_task()`: Carrega dados no PostgreSQL
- `upload_to_dell_ecs_task()`: Faz backup no Dell ECS

#### loaders/
Responsáveis por carregar dados no banco de dados:
- **base_loader.py**: Classe abstrata base
- **loader_factory.py**: Factory pattern para criar loaders
- **{provider}_loader.py**: Implementações específicas por provedor

#### storages/
Responsáveis por interagir com object storage:
- **storage_factory.py**: Factory pattern para criar storages
- **{provider}_storage.py**: Implementações específicas por provedor (AWS S3, OCI, GCP, Huawei OBS)
- **ecs_storage.py**: Storage para backup/auditoria (suporta Dell ECS, MinIO, S3-compatible)

#### utils/
Utilitários compartilhados:
- **files.py**: Compressão/descompressão (.gz, .zip), limpeza de diretórios
- **database.py**: Conexões e queries no PostgreSQL
- **dataframe.py**: Transformações em DataFrames pandas
- **logging_config.py**: Logger estruturado com contexto
- **dag_callbacks.py**: Callbacks de sucesso/falha das DAGs

#### interfaces/
Contratos e interfaces:
- **storage_interface.py**: Interface abstrata para storages

### Arquivos de Configuração

- **requirements.txt**: Lista todas as dependências Python necessárias
- **.gitignore**: Especifica arquivos que não devem ser versionados
- **airflow_variables.example.json**: Template de variáveis do Airflow

### Documentação

- **README.md**: Visão geral completa do projeto
- **QUICKSTART.md**: Guia passo a passo para configurar e rodar
- **CONTRIBUTING.md**: Guia para contribuidores
- **CHANGELOG.md**: Histórico de versões e mudanças
- **STRUCTURE.md**: Descrição da estrutura (este arquivo)
- **LICENSE**: Termos da licença do projeto

## Fluxo de Dados

```
┌──────────────────────────────┐
│    Cloud Providers           │
│  ┌────────┬────────┬────────┐│
│  │   AWS  │  OCI   │  GCP   ││
│  │   S3   │  OBS   │ Storage││
│  └────────┴────────┴────────┘│
│  ┌────────────────────────┐  │
│  │   Huawei Cloud OBS     │  │
│  └────────────────────────┘  │
└───────────────┬──────────────┘
                │ download_new_objects
                │ (reports FOCUS raw)
                ▼
┌──────────────────────────────┐
│    Temp Directory (Local)    │
│  (.gz, .zip, .csv files)     │
└───────┬──────────────┬───────┘
        │              │
        │ unpack_files │
        ▼              │
┌──────────────────┐   │
│  PostgreSQL DB   │   │ upload_to_backup_storage
│  (FOCUS tables)  │   │ (arquivos raw)
│  - focus_aws     │   │
│  - focus_oci     │   ▼
│  - focus_gcp     │  ┌───────────────────────────┐
│  - focus_hpc     │  │  Backup Storage           │
└──────────────────┘  │  (S3-Compatible)          │
                      │                           │
                      │  Opções:                  │
                      │  • Dell ECS (on-prem)     │
                      │  • MinIO (on-prem)        │
                      │  • AWS S3                 │
                      │  • Wasabi, Backblaze, etc │
                      │                           │
                      │  Estrutura:               │
                      │  - raw/aws/               │
                      │  - raw/oci/               │
                      │  - raw/gcp/               │
                      │  - raw/huawei/            │
                      └───────────────────────────┘
```

**Observações importantes:**
- Os arquivos raw são armazenados no backup storage exatamente como foram baixados da nuvem
- Suporta qualquer object storage compatível com S3 (Dell ECS, MinIO, AWS S3, etc)
- O backup storage serve como data lake para auditoria e reprocessamento
- PostgreSQL contém os dados transformados e estruturados no padrão FOCUS

## Padrões de Design

1. **Factory Pattern**: Usado em `loader_factory.py` e `storage_factory.py` para criar instâncias apropriadas baseadas no provedor

2. **Strategy Pattern**: Cada loader/storage implementa sua própria estratégia de processamento

3. **Template Method**: `base_loader.py` define o template para loaders específicos

4. **Dependency Injection**: Tasks recebem dependências via parâmetros

## Extensibilidade

Para adicionar um novo provedor:
1. Crie `{provider}_loader.py` em `loaders/`
2. Crie `{provider}_storage.py` em `storages/`
3. Atualize os factories
4. Crie a DAG em `dags/`
5. Atualize a documentação

Veja [CONTRIBUTING.md](CONTRIBUTING.md) para detalhes.
