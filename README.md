# Airflow FOCUS Pipeline

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Oracle Cloud](https://img.shields.io/badge/Oracle%20Cloud-Infrastructure-F80000?style=flat&logo=oracle&logoColor=white)](https://www.oracle.com/cloud/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Platform-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![Huawei Cloud](https://img.shields.io/badge/Huawei-Cloud-FF0000?style=flat&logo=huawei&logoColor=white)](https://www.huaweicloud.com/)
[![Dell ECS](https://img.shields.io/badge/Dell-ECS-007DB8?style=flat&logo=dell&logoColor=white)](https://www.dell.com/en-us/dt/storage/ecs/index.htm)
[![FOCUS](https://img.shields.io/badge/FinOps-FOCUS-00C853?style=flat)](https://focus.finops.org/)
[![License](https://img.shields.io/badge/License-View-blue?style=flat)](./LICENSE)

Pipeline ETL no Apache Airflow para coleta e processamento de relatórios de custos no padrão **FOCUS** (FinOps Open Cost and Usage Specification) dos principais provedores de nuvem.

---

## 📚 Documentação

- **[📑 Índice Completo da Documentação](DOCUMENTATION_INDEX.md)** - Navegue por toda a documentação
- **[🎯 Escopo e Premissas](SCOPE.md)** - O que está e o que NÃO está incluído
- **[🚀 Guia de Início Rápido](QUICKSTART.md)** - Configure e execute em minutos
- **[🏗️ Estrutura do Projeto](STRUCTURE.md)** - Entenda a arquitetura
- **[🤝 Guia de Contribuição](CONTRIBUTING.md)** - Como contribuir
- **[📝 Changelog](CHANGELOG.md)** - Histórico de versões

---

## Visão Geral

Este repositório contém DAGs do Apache Airflow que automatizam a coleta, transformação e armazenamento de dados de custos de cloud computing no formato FOCUS padronizado.

Os reports são coletados dos buckets na nuvem no seu **formato raw (original)** e armazenados em um **object storage S3-compatible** para backup e auditoria, garantindo controle total sobre os dados de custos.

---

## 🎯 Escopo e Premissas

### ✅ O que ESTE repositório contém

Este repositório foca **exclusivamente no processo ETL** dos reports FOCUS:

1. **📥 Coleta**: Download dos reports FOCUS dos buckets dos provedores
2. **🔄 Transformação**: Processamento e padronização dos dados
3. **💾 Carga**: Armazenamento no PostgreSQL (formato FOCUS)
4. **📦 Backup**: Sincronização dos arquivos raw para storage de auditoria

### ❌ O que NÃO está incluído neste repositório

Os seguintes tópicos **não são abordados** aqui e devem ser tratados separadamente:

#### 1. Infraestrutura do Apache Airflow
- ❌ Instalação e configuração do Airflow
- ❌ Deploy em Kubernetes/Docker
- ❌ Configuração de workers e executors
- ❌ Setup de logs e monitoring
- ❌ Configuração de autenticação e RBAC

> **Nota**: Para deploy em containers/Kubernetes, você precisará configurar **volumes persistentes** para os diretórios temporários de cada DAG (veja seção [Volumes Persistentes](#volumes-persistentes-para-containerskubernetes))

#### 2. Configuração de Data Exports nos Provedores
- ❌ Configuração de Cost and Usage Reports (AWS)
- ❌ Configuração de Cost Reports (OCI)
- ❌ Configuração de BigQuery Billing Export (GCP)
- ❌ Configuração de Cost Center (Huawei Cloud)

> **Premissa**: Os **Data Exports já estão configurados** e gerando reports FOCUS nos buckets dos provedores

#### 3. Geração de Reports no Padrão FOCUS
- ❌ Transformação de dados nativos para formato FOCUS
- ❌ Funções serverless para conversão (ex: GCP Cloud Functions)
- ❌ Mapeamento de campos nativos → FOCUS
- ❌ Normalização de taxonomias

> **Premissa**: Os reports **já estão no formato FOCUS** nos buckets dos provedores

### 📋 Premissas Importantes

Este projeto assume que:

1. **Reports FOCUS disponíveis**: Cada provedor já possui reports no formato FOCUS armazenados em seus respectivos buckets
2. **Buckets acessíveis**: Você possui credenciais válidas para acessar os buckets de cada provedor
3. **PostgreSQL configurado**: Banco de dados PostgreSQL já instalado e acessível
4. **Airflow operacional**: Apache Airflow já instalado e funcionando
5. **Backup storage disponível**: Object storage S3-compatible configurado para backup

---

## 🔧 Volumes Persistentes para Containers/Kubernetes

Se você está executando o Airflow em **containers (Docker)** ou **Kubernetes**, é **essencial** configurar volumes persistentes para os diretórios temporários de cada DAG.

### Por que volumes persistentes são necessários?

As DAGs processam arquivos grandes em múltiplas tasks sequenciais. Se o diretório temporário não for persistente:
- ❌ Arquivos baixados na task `download_new_objects` serão perdidos
- ❌ Task `unpack_files` não encontrará os arquivos
- ❌ Task `load_to_db` falhará por falta de dados
- ❌ Pipeline será interrompido

### Diretórios temporários por DAG

| DAG | Diretório Temporário | Volume Recomendado |
|-----|---------------------|-------------------|
| `aws_focus_pipeline` | `/tmp/aws_downloads` | `aws-focus-temp-pv` |
| `oci_focus_pipeline` | `/tmp/oci_downloads` | `oci-focus-temp-pv` |
| `gcp_focus_pipeline` | `/tmp/gcp_downloads` | `gcp-focus-temp-pv` |
| `hpc_focus_pipeline` | `/tmp/hpc_downloads` | `hpc-focus-temp-pv` |

### Exemplo de configuração Kubernetes

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: aws-focus-temp-pvc
  namespace: airflow
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi  # Ajuste conforme volume de dados
  storageClassName: standard
```

### Exemplo de configuração Docker Compose

```yaml
services:
  airflow-worker:
    volumes:
      - aws-focus-temp:/tmp/aws_downloads
      - oci-focus-temp:/tmp/oci_downloads
      - gcp-focus-temp:/tmp/gcp_downloads
      - hpc-focus-temp:/tmp/hpc_downloads

volumes:
  aws-focus-temp:
  oci-focus-temp:
  gcp-focus-temp:
  hpc-focus-temp:
```

> **💡 Dica**: Dimensione os volumes com base no tamanho médio dos seus reports FOCUS. Recomenda-se pelo menos 50-100GB por provedor.

---

### Provedores Suportados

- **AWS** (Amazon Web Services)
- **OCI** (Oracle Cloud Infrastructure)
- **GCP** (Google Cloud Platform)
- **Huawei Cloud**

### Backup Storage - Object Storage para Auditoria

O projeto utiliza um **Object Storage S3-compatible** para backup e auditoria dos reports de custos:

- 🔒 **Mantém cópia dos reports** de custos no formato raw/original
- 💾 **Armazena os arquivos brutos** (.gz, .zip, .csv) baixados dos buckets na nuvem
- 🏢 **Garante controle corporativo** sobre os dados de billing e custos
- 📦 **Serve como backup** dos reports originais antes da transformação
- 🔄 **Permite reprocessamento** dos dados a partir dos arquivos originais
- 🔐 **Atende requisitos de auditoria** mantendo histórico completo

#### Provedores de Storage Suportados

Qualquer **object storage compatível com S3** pode ser usado:

| Tipo | Deployment | Exemplos |
|------|------------|----------|
| **On-Premises** | Local, fora da nuvem | Dell ECS, MinIO, Ceph |
| **Cloud** | Provedores de nuvem | AWS S3, Wasabi, Backblaze B2 |
| **Híbrido** | Misto | MinIO distribuído, StorageGRID |

Após o processamento e carga no PostgreSQL, os arquivos originais são sincronizados para o storage de backup, criando um data lake completo dos reports de custos.

## Arquitetura

### Estrutura do Projeto

```
airflow_focus/
├── dags/                           # DAGs do Airflow
│   ├── aws_focus_pipeline_dag.py
│   ├── oci_focus_pipeline_dag.py
│   ├── gcp_focus_pipeline_dag.py
│   └── hpc_focus_pipeline_dag.py   # Huawei Cloud
├── plugins/                        # Plugins e módulos compartilhados
│   ├── tasks.py                    # Funções de tarefas principais
│   ├── loaders/                    # Carregadores específicos por provedor
│   │   ├── aws_loader.py
│   │   ├── oci_loader.py
│   │   ├── gcp_loader.py
│   │   ├── hpc_loader.py
│   │   ├── base_loader.py
│   │   └── loader_factory.py
│   ├── storages/                   # Integrações com storage
│   │   ├── aws_storage.py
│   │   ├── oci_storage.py
│   │   ├── gcp_storage.py
│   │   ├── hpc_storage.py
│   │   ├── ecs_storage.py
│   │   └── storage_factory.py
│   ├── utils/                      # Utilitários
│   │   ├── files.py                # Manipulação de arquivos
│   │   ├── database.py             # Operações de banco de dados
│   │   ├── dag_callbacks.py        # Callbacks das DAGs
│   │   └── logging_config.py       # Configuração de logs
│   └── interfaces/                 # Interfaces e contratos
├── requirements.txt
└── README.md
```

### Fluxo de Processamento

Cada DAG FOCUS segue um fluxo ETL padronizado:

```
1. list_processed_keys
   ↓
2. download_new_objects
   ↓
3. unpack_files (quando necessário)
   ↓
4. load_to_db
   ↓
5. upload_to_dell_ecs
   ↓
6. clean_temp_dir
```

#### Descrição das Etapas

1. **list_processed_keys**: Lista os arquivos já processados no banco de dados para evitar duplicação
2. **download_new_objects**: Baixa novos objetos do bucket do provedor na nuvem
3. **unpack_files**: Descompacta arquivos (.gz para AWS/OCI, .zip para Huawei, não necessário para GCP)
4. **load_to_db**: Carrega dados no PostgreSQL seguindo o esquema FOCUS
5. **upload_to_dell_ecs**: Sincroniza os arquivos raw para o backup storage (S3-compatible)
6. **clean_temp_dir**: Limpa o diretório temporário local

## Configuração

### Pré-requisitos

- Apache Airflow 2.7+
- PostgreSQL 12+
- Python 3.10+
- Credenciais configuradas para cada provedor de nuvem

### Instalação

1. Clone o repositório:
```bash
git clone <repository-url>
cd airflow_focus
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure as variáveis do Airflow.

**Recomendado:** Use o arquivo JSON para importar todas as variáveis:

```bash
# Copie o arquivo de exemplo
cp airflow_variables.example.json airflow_variables.json

# Edite com seus valores reais
nano airflow_variables.json

# Importe para o Airflow
airflow variables import airflow_variables.json
```

**Alternativa:** Configure via CLI (nomes exatamente como lidos pelo código via `Variable.get()`):

```bash
# AWS - Credenciais e Bucket
airflow variables set AWS_ACCESS_KEY_ID "sua-access-key"
airflow variables set AWS_SECRET_ACCESS_KEY "sua-secret-key"
airflow variables set AWS_BUCKET "seu-bucket-aws"
airflow variables set AWS_PREFIX "reports/focus/"

# OCI - Namespace e Bucket (credenciais via arquivo ./config/oci.conf)
airflow variables set OCI_NAMESPACE "seu-namespace-oci"
airflow variables set OCI_BUCKET "seu-bucket-oci"
airflow variables set OCI_PREFIX "reports/focus/"

# GCP - Bucket (credenciais via chaves HMAC da API S3-compatible do Cloud Storage)
airflow variables set GCP_ACCESS_KEY "sua-gcp-hmac-access-key"
airflow variables set GCP_SECRET_KEY "sua-gcp-hmac-secret-key"
airflow variables set GCP_BUCKET "seu-bucket-gcp"
airflow variables set GCP_PREFIX "reports/focus/"

# Huawei Cloud (OBS) - Credenciais e Bucket
airflow variables set HPC_ACCESS_KEY "sua-huawei-access-key"
airflow variables set HPC_SECRET_KEY "sua-huawei-secret-key"
airflow variables set HPC_SERVER "https://obs.sa-brazil-1.myhuaweicloud.com"
airflow variables set HPC_BUCKET "seu-bucket-huawei"
airflow variables set HPC_FOCUS_PREFIX "reports/focus/"

# Backup Storage (S3-Compatible) - Dell ECS, MinIO, etc
airflow variables set ECS_ACCESS_KEY "sua-ecs-access-key"
airflow variables set ECS_SECRET_KEY "sua-ecs-secret-key"
airflow variables set ECS_ENDPOINT_URL "https://seu-storage.exemplo.com"
airflow variables set ECS_BUCKET "focus-reports-backup"
airflow variables set ECS_AWS_PREFIX "raw/aws/"
airflow variables set ECS_OCI_PREFIX "raw/oci/"
airflow variables set ECS_GCP_PREFIX "raw/gcp/"
airflow variables set ECS_HPC_FOCUS_PREFIX "raw/huawei/"
```

> **Nota sobre Backup Storage**: Pode ser Dell ECS, MinIO, AWS S3, Wasabi, ou qualquer object storage compatível com S3. `ECS_ENDPOINT_URL` é sempre obrigatório, pois é lido diretamente pelo código (use o endpoint do seu storage; para AWS S3 use `https://s3.amazonaws.com` ou a URL regional equivalente).

4. Configure as credenciais dos provedores:

**AWS**: Configure através de variáveis de ambiente ou arquivo `~/.aws/credentials`
```bash
export AWS_ACCESS_KEY_ID="sua-access-key"
export AWS_SECRET_ACCESS_KEY="sua-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

**OCI**: Crie o arquivo `./config/oci.conf` (caminho relativo ao diretório de trabalho do worker do Airflow — veja `plugins/storages/oci_storage.py`)
```ini
[DEFAULT]
user=ocid1.user.oc1...
fingerprint=aa:bb:cc:dd...
tenancy=ocid1.tenancy.oc1...
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
```
> O namespace do bucket é lido da variável do Airflow `OCI_NAMESPACE`, não do arquivo de config.

**GCP**: O storage GCP usa a API S3-compatible do Cloud Storage com chaves HMAC, configuradas via variáveis do Airflow `GCP_ACCESS_KEY` e `GCP_SECRET_KEY` (veja seção de Variáveis acima). Não é necessário `GOOGLE_APPLICATION_CREDENTIALS`.

**Huawei**: Configure através das variáveis do Airflow `HPC_ACCESS_KEY`, `HPC_SECRET_KEY` e `HPC_SERVER` (veja seção de Variáveis acima).

5. Configure as variáveis do banco de dados:

```bash
# PostgreSQL - Configuração de Conexão
airflow variables set FINOPS_DB_HOST "localhost"
airflow variables set FINOPS_DB "finops"
airflow variables set FINOPS_USER "airflow_user"
airflow variables set FINOPS_PASSWORD "sua-senha-segura"
```

> **Importante**: A conexão (host, usuário, senha, database) é configurável via variáveis do Airflow. A porta (`5432`) e os nomes das tabelas FOCUS (`tb_aws_focus`, `tb_oci_focus`, `tb_gcp_focus`, `tb_hpc_focus`, `tb_processed_files`) são fixos no código (`plugins/utils/database.py` e `plugins/loaders/`), não configuráveis via variável.

### Estrutura do Banco de Dados

Cada provedor armazena seus dados em uma tabela específica seguindo o esquema FOCUS:

- **AWS**: `tb_aws_focus`
- **OCI**: `tb_oci_focus`
- **GCP**: `tb_gcp_focus`
- **Huawei**: `tb_hpc_focus`

Os arquivos já processados são rastreados na tabela `tb_processed_files`.

## Uso

### Ativar as DAGs

Por padrão, todas as DAGs são criadas pausadas (`is_paused_upon_creation=True`). Para ativar:

```bash
# Ativar todas as DAGs FOCUS
airflow dags unpause aws_focus_pipeline
airflow dags unpause oci_focus_pipeline
airflow dags unpause gcp_focus_pipeline
airflow dags unpause hpc_focus_pipeline
```

### Executar Manualmente

```bash
# Executar uma DAG específica
airflow dags trigger aws_focus_pipeline
```

### Agendamento

Cada DAG tem seu próprio agendamento otimizado:

| Provedor | Schedule | Descrição |
|----------|----------|-----------|
| AWS | `0 10,16,22,4 * * *` | 4x ao dia (10h, 16h, 22h, 4h) |
| OCI | `0 */2 * * *` | A cada 2 horas |
| GCP | `0 10 * * *` | Diariamente às 10h |
| Huawei | `0 6 * * *` | Diariamente às 6h |

## Formato dos Dados

Os dados são processados e armazenados seguindo o padrão **FOCUS** (FinOps Open Cost and Usage Specification), que padroniza os campos de custos entre diferentes provedores:

### Principais Campos FOCUS

- `billing_period_start`/`billing_period_end`: Período de cobrança
- `charge_type`: Tipo de cobrança (Usage, Tax, Credit, etc.)
- `charge_category`: Categoria da cobrança
- `service_name`: Nome do serviço utilizado
- `resource_id`: Identificador único do recurso
- `region`: Região do recurso
- `billed_cost`: Custo cobrado
- `pricing_unit`: Unidade de precificação

## Características Específicas por Provedor

### AWS
- **Formato dos arquivos**: `.gz` (Parquet comprimido)
- **Descompactação**: Sim
- **Frequência de atualização**: 4x ao dia

### OCI
- **Formato dos arquivos**: `.gz` (CSV comprimido)
- **Descompactação**: Sim
- **Frequência de atualização**: A cada 2 horas

### GCP
- **Formato dos arquivos**: `.csv` (não comprimido)
- **Descompactação**: Não necessária
- **Frequência de atualização**: Diária

### Huawei Cloud
- **Formato dos arquivos**: `.zip` (CSV comprimido)
- **Descompactação**: Sim
- **Frequência de atualização**: Diária

## Monitoramento

### Logs

Todas as tarefas utilizam logging estruturado através do módulo `utils.logging_config`. Os logs incluem:

- Métricas de performance (tempo de execução, número de arquivos processados)
- Contexto de execução (provider, task_id, dag_id)
- Rastreamento de erros completo

### Callbacks

As DAGs implementam callbacks de sucesso e falha:

- `on_dag_success`: Executado quando a DAG completa com sucesso
- `on_dag_failure`: Executado quando a DAG falha

Você pode customizar estes callbacks em `plugins/utils/dag_callbacks.py`.

## Troubleshooting

### Erro de Conexão com Bucket

**Sintoma**: `NoCredentialsError` ou `AuthenticationFailed`

**Solução**: Verifique se as credenciais do provedor estão configuradas corretamente.

### Arquivo Já Processado

**Sintoma**: Arquivos não são baixados novamente

**Comportamento esperado**: O sistema mantém registro dos arquivos processados no banco de dados para evitar duplicação.

**Como reprocessar**: Delete o registro do arquivo na tabela `processed_files` do banco de dados.

### Diretório Temporário Cheio

**Sintoma**: `No space left on device`

**Solução**: A task `clean_temp_dir` deveria limpar automaticamente. Verifique se está sendo executada com sucesso. Se necessário, limpe manualmente:
```bash
rm -rf /tmp/{aws,oci,gcp,hpc}_downloads/*
```

## Desenvolvimento

### Adicionando um Novo Provedor

1. Crie um loader em `plugins/loaders/novo_provider_loader.py`
2. Crie um storage em `plugins/storages/novo_provider_storage.py`
3. Atualize os factories (`loader_factory.py` e `storage_factory.py`)
4. Crie uma DAG em `dags/novo_provider_focus_pipeline_dag.py`
5. Configure as variáveis necessárias no Airflow

### Testes

Para executar testes locais (sem Airflow):

```bash
pip install pytest hypothesis
python -m pytest
```

## Contribuindo

Contribuições são bem-vindas! Por favor, leia o [Guia de Contribuição](CONTRIBUTING.md) para detalhes sobre nosso código de conduta e o processo de submissão de pull requests.

### Processo Rápido

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

Para mais detalhes sobre como adicionar novos provedores, padrões de código e testes, consulte o [CONTRIBUTING.md](CONTRIBUTING.md).

## Licença

Este projeto está licenciado sob os termos da licença especificada no arquivo LICENSE.

## Suporte

Para questões e suporte, abra uma issue no repositório do projeto.

## Referências

- [FinOps Open Cost and Usage Specification (FOCUS)](https://focus.finops.org/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Cost and Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/)
- [OCI Cost Reports](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/usagereportsoverview.htm)
- [GCP Cloud Billing Export](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- [Huawei Cloud Cost Center](https://support.huaweicloud.com/intl/en-us/usermanual-billing/)
