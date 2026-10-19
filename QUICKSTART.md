# Guia de Início Rápido

Este guia te ajudará a configurar e executar o Airflow FOCUS Pipeline em poucos passos.

## Pré-requisitos

- Python 3.10+
- PostgreSQL 12+
- Apache Airflow 2.7+
- Credenciais de acesso aos provedores de nuvem (AWS, OCI, GCP, Huawei)

## Passo 1: Instalação

```bash
# Clone o repositório
git clone <repository-url>
cd airflow_focus

# Crie e ative um ambiente virtual
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate  # Windows

# Instale as dependências
pip install -r requirements.txt
```

## Passo 2: Configurar Airflow

### 2.1 Inicializar o Airflow

```bash
# Defina o diretório home do Airflow
export AIRFLOW_HOME=$(pwd)/airflow_home

# Inicialize o banco de dados do Airflow
airflow db init

# Crie um usuário admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 2.2 Configurar as DAGs e Plugins

```bash
# Crie links simbólicos ou copie os diretórios
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/plugins

# Opção 1: Links simbólicos (recomendado para desenvolvimento)
ln -s $(pwd)/dags/* $AIRFLOW_HOME/dags/
ln -s $(pwd)/plugins/* $AIRFLOW_HOME/plugins/

# Opção 2: Copiar arquivos (para produção)
cp -r dags/* $AIRFLOW_HOME/dags/
cp -r plugins/* $AIRFLOW_HOME/plugins/
```

## Passo 3: Configurar Banco de Dados PostgreSQL

### 3.1 Criar banco de dados

```sql
-- Conecte-se ao PostgreSQL como superusuário
psql -U postgres

-- Crie o banco de dados
CREATE DATABASE finops;

-- Crie o usuário
CREATE USER airflow_user WITH PASSWORD 'sua_senha_segura';

-- Conceda permissões
GRANT ALL PRIVILEGES ON DATABASE finops TO airflow_user;

-- Conecte-se ao banco finops
\c finops

-- Conceda permissões no schema public
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO airflow_user;
```

### 3.2 Configurar conexão no Airflow

```bash
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-schema 'finops' \
    --conn-login 'airflow_user' \
    --conn-password 'sua_senha_segura' \
    --conn-port 5432
```

## Passo 4: Configurar Credenciais dos Provedores

### AWS

```bash
# Opção 1: Variáveis de ambiente
export AWS_ACCESS_KEY_ID="sua-access-key"
export AWS_SECRET_ACCESS_KEY="sua-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Opção 2: Arquivo de credenciais
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = sua-access-key
aws_secret_access_key = sua-secret-key
EOF

cat > ~/.aws/config << EOF
[default]
region = us-east-1
EOF
```

### OCI (Oracle Cloud)

```bash
# Crie o diretório de configuração
mkdir -p ~/.oci

# Crie o arquivo de configuração
cat > ~/.oci/config << EOF
[DEFAULT]
user=ocid1.user.oc1..<seu-user-ocid>
fingerprint=<seu-fingerprint>
tenancy=ocid1.tenancy.oc1..<seu-tenancy-ocid>
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
EOF

# Copie sua chave privada
cp /caminho/para/sua/chave.pem ~/.oci/oci_api_key.pem
chmod 600 ~/.oci/oci_api_key.pem
```

### GCP (Google Cloud)

```bash
# Crie o diretório de configuração
mkdir -p ~/.gcp

# Copie o arquivo de service account
cp /caminho/para/service-account-key.json ~/.gcp/service-account-key.json

# Configure a variável de ambiente
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gcp/service-account-key.json"
```

### Huawei Cloud

```bash
# Configure as variáveis de ambiente
export HUAWEI_ACCESS_KEY_ID="sua-access-key"
export HUAWEI_SECRET_ACCESS_KEY="sua-secret-key"
export HUAWEI_REGION="sa-brazil-1"
```

## Passo 5: Configurar Variáveis do Airflow

### 5.1 Através do arquivo JSON

```bash
# Copie o arquivo de exemplo
cp airflow_variables.example.json airflow_variables.json

# Edite com seus valores
nano airflow_variables.json

# Importe as variáveis
airflow variables import airflow_variables.json
```

### 5.2 Através da CLI (alternativa)

```bash
# AWS - Credenciais e Configuração
airflow variables set AWS_ACCESS_KEY_ID "AKIAIOSFODNN7EXAMPLE"
airflow variables set AWS_SECRET_ACCESS_KEY "sua-secret-key"
airflow variables set AWS_REGION "us-east-1"
airflow variables set AWS_BUCKET "seu-bucket-aws"
airflow variables set AWS_PREFIX "reports/focus/"

# OCI - Configuração do Bucket
# Nota: OCI usa ~/.oci/config para credenciais
airflow variables set OCI_BUCKET "seu-bucket-oci"
airflow variables set OCI_PREFIX "reports/focus/"

# GCP - Configuração do Bucket
# Nota: GCP usa service account JSON (GOOGLE_APPLICATION_CREDENTIALS)
airflow variables set GCP_BUCKET "seu-bucket-gcp"
airflow variables set GCP_PREFIX "reports/focus/"
airflow variables set GCP_PROJECT_ID "seu-projeto-gcp"

# Huawei Cloud - Credenciais e Configuração
airflow variables set HUAWEI_ACCESS_KEY_ID "sua-huawei-access-key"
airflow variables set HUAWEI_SECRET_ACCESS_KEY "sua-huawei-secret-key"
airflow variables set HUAWEI_REGION "sa-brazil-1"
airflow variables set HPC_BUCKET "seu-bucket-huawei"
airflow variables set HPC_FOCUS_PREFIX "reports/focus/"

# Backup Storage (S3-Compatible)
# Suporta: Dell ECS, MinIO, AWS S3, Wasabi, Backblaze B2, etc
airflow variables set BACKUP_STORAGE_TYPE "ecs"
airflow variables set BACKUP_ACCESS_KEY_ID "sua-backup-access-key"
airflow variables set BACKUP_SECRET_ACCESS_KEY "sua-backup-secret-key"
airflow variables set BACKUP_ENDPOINT_URL "https://seu-ecs.exemplo.com"
airflow variables set BACKUP_BUCKET "focus-reports-backup"
airflow variables set BACKUP_AWS_PREFIX "raw/aws/"
airflow variables set BACKUP_OCI_PREFIX "raw/oci/"
airflow variables set BACKUP_GCP_PREFIX "raw/gcp/"
airflow variables set BACKUP_HUAWEI_PREFIX "raw/huawei/"
airflow variables set BACKUP_REGION "us-east-1"
```

**Nota sobre Backup Storage:**
- **Dell ECS**: Configure `BACKUP_ENDPOINT_URL` com o endereço do ECS
- **MinIO**: Configure `BACKUP_ENDPOINT_URL` com o endereço do MinIO
- **AWS S3**: Não precisa de `BACKUP_ENDPOINT_URL`, apenas credenciais
- **Outros S3-compatible**: Configure o endpoint específico do provedor

# PostgreSQL Database - Configuração de Conexão
airflow variables set FINOPS_DB_HOST "localhost"
airflow variables set FINOPS_DB_PORT "5432"
airflow variables set FINOPS_DB "finops"
airflow variables set FINOPS_USER "airflow_user"
airflow variables set FINOPS_PASSWORD "sua-senha-segura"

# PostgreSQL Database - Nomes das Tabelas FOCUS
airflow variables set TABLE_AWS_FOCUS "tb_aws_focus"
airflow variables set TABLE_OCI_FOCUS "tb_oci_focus"
airflow variables set TABLE_GCP_FOCUS "tb_gcp_focus"
airflow variables set TABLE_HUAWEI_FOCUS "tb_hpc_focus"
airflow variables set TABLE_PROCESSED_FILES "tb_processed_files"
```

> **⚠️ Importante**: Nenhuma configuração está hard-coded no código. Todos os parâmetros são configuráveis via variáveis do Airflow.

## Passo 6: Iniciar o Airflow

### Opção 1: Modo Standalone (desenvolvimento)

```bash
# Inicie o Airflow em modo standalone
airflow standalone
```

Acesse: http://localhost:8080

### Opção 2: Modo Produção

```bash
# Terminal 1: Webserver
airflow webserver --port 8080

# Terminal 2: Scheduler
airflow scheduler
```

## Passo 7: Ativar e Executar as DAGs

### Via Interface Web

1. Acesse http://localhost:8080
2. Faça login com as credenciais criadas
3. Na lista de DAGs, encontre as DAGs FOCUS:
   - `aws_focus_pipeline`
   - `oci_focus_pipeline`
   - `gcp_focus_pipeline`
   - `hpc_focus_pipeline`
4. Ative cada DAG clicando no toggle
5. Clique no botão "Play" para executar manualmente

### Via CLI

```bash
# Ativar as DAGs
airflow dags unpause aws_focus_pipeline
airflow dags unpause oci_focus_pipeline
airflow dags unpause gcp_focus_pipeline
airflow dags unpause hpc_focus_pipeline

# Executar manualmente (teste)
airflow dags trigger aws_focus_pipeline
```

## Passo 8: Monitorar a Execução

### Via Interface Web

1. Clique no nome da DAG
2. Visualize o grafo de execução
3. Clique nas tarefas para ver logs

### Via CLI

```bash
# Listar execuções da DAG
airflow dags list-runs -d aws_focus_pipeline

# Ver logs de uma task específica
airflow tasks logs aws_focus_pipeline list_processed_keys <execution_date>
```

## Verificação da Instalação

Execute este checklist para garantir que tudo está configurado corretamente:

```bash
# 1. Verificar se o Airflow está rodando
curl http://localhost:8080/health

# 2. Listar DAGs disponíveis
airflow dags list | grep focus

# 3. Verificar variáveis configuradas
airflow variables list | grep -E "AWS_|OCI_|GCP_|HPC_|HUAWEI_|BACKUP_"

# 4. Verificar conexão com PostgreSQL
airflow connections get postgres_default

# 5. Testar uma task específica
airflow tasks test aws_focus_pipeline list_processed_keys 2024-01-01
```

## Solução de Problemas Comuns

### DAGs não aparecem na interface

```bash
# Verifique se os diretórios estão corretos
echo $AIRFLOW_HOME
ls -la $AIRFLOW_HOME/dags/
ls -la $AIRFLOW_HOME/plugins/

# Force a recarga das DAGs
airflow dags reserialize
```

### Erro de importação de módulos

```bash
# Verifique se os plugins estão no Python path
export PYTHONPATH=$PYTHONPATH:$AIRFLOW_HOME/plugins

# Adicione ao ~/.bashrc ou ~/.zshrc para persistir
echo 'export PYTHONPATH=$PYTHONPATH:$AIRFLOW_HOME/plugins' >> ~/.bashrc
```

### Erro de credenciais

```bash
# Verifique se as variáveis de ambiente estão definidas
env | grep -E "AWS_|GOOGLE_|HUAWEI_"

# Teste a conexão com AWS
aws s3 ls

# Teste a conexão com GCP
gcloud auth application-default print-access-token
```

## Próximos Passos

1. **Personalizar agendamento**: Edite os arquivos das DAGs para ajustar os schedules
2. **Configurar alertas**: Configure o `on_failure_callback` em `dag_callbacks.py`
3. **Monitoramento**: Configure ferramentas de monitoramento (Prometheus, Grafana)
4. **Backup**: Configure backups automáticos do banco de dados PostgreSQL
5. **Escalabilidade**: Configure Celery ou Kubernetes executor para processar múltiplas DAGs em paralelo

## Recursos Adicionais

- [Documentação completa](README.md)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [FOCUS Specification](https://focus.finops.org/)

## Suporte

Se você encontrar problemas, verifique:
1. Logs do Airflow em `$AIRFLOW_HOME/logs/`
2. Logs das tarefas na interface web
3. Conexão com o banco de dados
4. Credenciais dos provedores

Para mais ajuda, abra uma issue no repositório do projeto.
