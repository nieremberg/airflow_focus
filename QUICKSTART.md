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

O client OCI (`plugins/storages/oci_storage.py`) carrega as credenciais de um arquivo de config em um caminho relativo (`./config/oci.conf`, relativo ao diretório de trabalho do processo do Airflow worker) — não de `~/.oci/config`.

```bash
# Crie o diretório de configuração relativo ao AIRFLOW_HOME (ou cwd do worker)
mkdir -p ./config

# Crie o arquivo de configuração
cat > ./config/oci.conf << EOF
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

> O namespace do bucket OCI é configurado separadamente via variável do Airflow `OCI_NAMESPACE` (veja Passo 5).

### GCP (Google Cloud)

O storage GCP (`plugins/storages/gcp_storage.py`) acessa o Cloud Storage via API S3-compatible usando **chaves HMAC**, não um service account JSON. Gere um par de chaves HMAC no console do GCP e configure-as como variáveis do Airflow `GCP_ACCESS_KEY` / `GCP_SECRET_KEY` (veja Passo 5) — não é necessário `GOOGLE_APPLICATION_CREDENTIALS`.

### Huawei Cloud

As credenciais do Huawei OBS (`plugins/storages/hpc_storage.py`) são lidas das variáveis do Airflow `HPC_ACCESS_KEY`, `HPC_SECRET_KEY` e `HPC_SERVER` (veja Passo 5) — não de variáveis de ambiente.

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
airflow variables set AWS_BUCKET "seu-bucket-aws"
airflow variables set AWS_PREFIX "reports/focus/"

# OCI - Namespace e Configuração do Bucket
# Nota: credenciais via arquivo ./config/oci.conf (veja seção de credenciais acima)
airflow variables set OCI_NAMESPACE "seu-namespace-oci"
airflow variables set OCI_BUCKET "seu-bucket-oci"
airflow variables set OCI_PREFIX "reports/focus/"

# GCP - Credenciais (chaves HMAC) e Configuração do Bucket
airflow variables set GCP_ACCESS_KEY "sua-gcp-hmac-access-key"
airflow variables set GCP_SECRET_KEY "sua-gcp-hmac-secret-key"
airflow variables set GCP_BUCKET "seu-bucket-gcp"
airflow variables set GCP_PREFIX "reports/focus/"

# Huawei Cloud (OBS) - Credenciais e Configuração
airflow variables set HPC_ACCESS_KEY "sua-huawei-access-key"
airflow variables set HPC_SECRET_KEY "sua-huawei-secret-key"
airflow variables set HPC_SERVER "https://obs.sa-brazil-1.myhuaweicloud.com"
airflow variables set HPC_BUCKET "seu-bucket-huawei"
airflow variables set HPC_FOCUS_PREFIX "reports/focus/"

# Backup Storage (S3-Compatible) - Dell ECS, MinIO, etc
airflow variables set ECS_ACCESS_KEY "sua-ecs-access-key"
airflow variables set ECS_SECRET_KEY "sua-ecs-secret-key"
airflow variables set ECS_ENDPOINT_URL "https://seu-ecs.exemplo.com"
airflow variables set ECS_BUCKET "focus-reports-backup"
airflow variables set ECS_AWS_PREFIX "raw/aws/"
airflow variables set ECS_OCI_PREFIX "raw/oci/"
airflow variables set ECS_GCP_PREFIX "raw/gcp/"
airflow variables set ECS_HPC_FOCUS_PREFIX "raw/huawei/"

# PostgreSQL Database - Configuração de Conexão
airflow variables set FINOPS_DB_HOST "localhost"
airflow variables set FINOPS_DB "finops"
airflow variables set FINOPS_USER "airflow_user"
airflow variables set FINOPS_PASSWORD "sua-senha-segura"
```

**Nota sobre Backup Storage:**
- `ECS_ENDPOINT_URL` é sempre lido pelo código e deve ser definido (para Dell ECS, MinIO ou qualquer S3-compatible, use o endpoint correspondente; para AWS S3 use `https://s3.amazonaws.com` ou a URL regional).

> **⚠️ Importante**: A porta do PostgreSQL (`5432`) e os nomes das tabelas FOCUS (`tb_aws_focus`, `tb_oci_focus`, `tb_gcp_focus`, `tb_hpc_focus`, `tb_processed_files`) são fixos no código, não configuráveis via variável do Airflow.

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
airflow variables list | grep -E "AWS_|OCI_|GCP_|HPC_|ECS_|FINOPS_"

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
# Todas as credenciais de storage (AWS, OCI, GCP, Huawei, ECS) são lidas via
# variáveis do Airflow, não variáveis de ambiente. Verifique se estão definidas:
airflow variables list | grep -E "AWS_|OCI_|GCP_|HPC_|ECS_"

# Exceção: o client OCI também depende do arquivo ./config/oci.conf existir
# e ser legível pelo processo do worker
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
