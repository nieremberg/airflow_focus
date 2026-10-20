# Escopo do Projeto - Airflow FOCUS Pipeline

Este documento define claramente **o que está incluído** e **o que NÃO está incluído** neste repositório.

---

## ✅ O QUE ESTE REPOSITÓRIO CONTÉM

### 1. DAGs do Apache Airflow para ETL de Reports FOCUS

Este repositório fornece **pipelines ETL completos** para processar reports no padrão FOCUS:

#### 📥 Coleta (Extract)
- Download de reports FOCUS dos buckets dos provedores de nuvem
- Suporte para AWS S3, OCI Object Storage, GCP Cloud Storage, Huawei OBS
- Tracking de arquivos já processados para evitar duplicação
- Download incremental de novos reports

#### 🔄 Transformação (Transform)
- Descompactação de arquivos (.gz, .zip)
- Processamento e validação de dados FOCUS
- Parsing de colunas datetime e JSON
- Limpeza e normalização de dados

#### 💾 Carga (Load)
- Armazenamento no PostgreSQL em tabelas FOCUS
- Upsert de dados (inserção ou atualização)
- Indexação e otimização de queries
- Rastreamento de processamento

#### 📦 Backup
- Sincronização de arquivos raw para storage de auditoria
- Suporte a qualquer object storage S3-compatible
- Organização hierárquica por provedor
- Preservação dos arquivos originais

### 2. Configuração Flexível via Variáveis

Tudo é configurável através de variáveis do Airflow:

- **Credenciais** dos provedores de nuvem
- **Configurações** de buckets e prefixes
- **Parâmetros** de conexão do banco de dados PostgreSQL (host, usuário, senha, database)
- **Endpoints** de backup storage

**Credenciais, buckets, prefixes e conexão com o banco não estão hard-coded** no código. A porta do PostgreSQL e os nomes das tabelas FOCUS são fixos no código (não configuráveis via variável).

### 3. Documentação Completa

- README com instruções detalhadas
- QUICKSTART para configuração rápida
- STRUCTURE explicando a arquitetura
- CONTRIBUTING para desenvolvedores
- Exemplos de configuração para diferentes cenários

---

## ❌ O QUE NÃO ESTÁ INCLUÍDO NESTE REPOSITÓRIO

### 1. Infraestrutura do Apache Airflow

**Não fornecemos:**
- ❌ Instalação do Apache Airflow
- ❌ Configuração de ambiente (produção/desenvolvimento)
- ❌ Deploy em Kubernetes ou Docker
- ❌ Configuração de Executors (LocalExecutor, CeleryExecutor, KubernetesExecutor)
- ❌ Setup de Workers e Schedulers
- ❌ Configuração de autenticação e RBAC
- ❌ Monitoramento e alertas
- ❌ Logging centralizado
- ❌ Backup e disaster recovery do Airflow

**Você deve:**
- ✅ Instalar e configurar o Airflow por conta própria
- ✅ Escolher e configurar o executor apropriado
- ✅ Configurar volumes persistentes (se usar containers)
- ✅ Implementar monitoramento e alertas
- ✅ Gerenciar a infraestrutura do Airflow

**Recursos úteis:**
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Deploying Airflow on Kubernetes](https://airflow.apache.org/docs/helm-chart/)
- [Production Deployment](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)

### 2. Configuração de Data Exports nos Provedores

**Não fornecemos:**
- ❌ Configuração de Cost and Usage Reports (AWS)
- ❌ Configuração de Cost Reports (OCI)
- ❌ Configuração de BigQuery Billing Export (GCP)
- ❌ Configuração de Cost Center Export (Huawei Cloud)
- ❌ Setup de buckets para armazenar os reports
- ❌ Configuração de permissões IAM/RBAC
- ❌ Definição de schedules de exportação

**Premissa:**
> ✅ Os Data Exports **já estão configurados** nos provedores e gerando reports nos buckets

**Você deve:**
- ✅ Configurar os Cost Reports em cada provedor
- ✅ Criar e configurar buckets para armazenar os reports
- ✅ Configurar permissões de acesso aos buckets
- ✅ Definir a periodicidade de geração dos reports

**Recursos úteis:**
- [AWS CUR Setup](https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html)
- [OCI Cost Reports](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/usagereportsoverview.htm)
- [GCP Billing Export](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- [Huawei Cost Analysis](https://support.huaweicloud.com/intl/en-us/usermanual-billing/)

### 3. Geração de Reports no Padrão FOCUS

**Não fornecemos:**
- ❌ Transformação de dados nativos dos provedores para formato FOCUS
- ❌ Cloud Functions / Lambda para conversão FOCUS
- ❌ Mapeamento de campos nativos → FOCUS
- ❌ Scripts de normalização de taxonomias
- ❌ Validação de conformidade FOCUS
- ❌ Enriquecimento de dados FOCUS

**Premissa:**
> ✅ Os reports **já estão no formato FOCUS** nos buckets dos provedores

**Você deve:**
- ✅ Implementar conversão para FOCUS (se necessário)
- ✅ Validar conformidade com a especificação FOCUS
- ✅ Garantir qualidade dos dados FOCUS

**Recursos úteis:**
- [FOCUS Specification](https://focus.finops.org/)
- [FOCUS GitHub](https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec)
- [FinOps Foundation](https://www.finops.org/)

### 4. Configuração do Banco de Dados PostgreSQL

**Não fornecemos:**
- ❌ Instalação do PostgreSQL
- ❌ Criação das tabelas FOCUS
- ❌ Definição dos esquemas de dados
- ❌ Criação de índices e otimizações
- ❌ Configuração de backup do banco
- ❌ Tuning de performance
- ❌ Configuração de replicação

**Premissa:**
> ✅ PostgreSQL já está instalado, acessível e configurado

**Você deve:**
- ✅ Instalar e configurar PostgreSQL
- ✅ Criar o banco de dados e as tabelas
- ✅ Configurar usuários e permissões
- ✅ Implementar estratégia de backup
- ✅ Otimizar queries e índices

**Recursos úteis:**
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Performance Tuning](https://wiki.postgresql.org/wiki/Performance_Optimization)

### 5. Configuração de Backup Storage

**Não fornecemos:**
- ❌ Instalação de Dell ECS, MinIO, etc
- ❌ Configuração de buckets no backup storage
- ❌ Setup de credenciais e políticas de acesso
- ❌ Configuração de lifecycle policies
- ❌ Retenção e arquivamento de dados

**Premissa:**
> ✅ Object storage S3-compatible já está configurado e acessível

**Você deve:**
- ✅ Escolher e configurar o backup storage
- ✅ Criar buckets e configurar permissões
- ✅ Definir políticas de retenção
- ✅ Implementar estratégia de backup

---

## 📋 PREMISSAS DO PROJETO

Para que este projeto funcione corretamente, assumimos que:

### 1. Reports FOCUS Disponíveis
- ✅ Cada provedor possui reports no formato FOCUS
- ✅ Reports estão armazenados em buckets acessíveis
- ✅ Reports seguem a especificação FOCUS corretamente

### 2. Credenciais Configuradas
- ✅ Você possui credenciais válidas para cada provedor
- ✅ Credenciais têm permissões de leitura nos buckets
- ✅ Credenciais estão configuradas no Airflow

### 3. PostgreSQL Operacional
- ✅ Banco de dados PostgreSQL instalado e rodando
- ✅ Tabelas FOCUS criadas e acessíveis
- ✅ Usuário do Airflow tem permissões de leitura/escrita

### 4. Apache Airflow Funcional
- ✅ Airflow instalado e operacional
- ✅ Scheduler e Webserver rodando
- ✅ Workers disponíveis para executar tasks

### 5. Backup Storage Configurado
- ✅ Object storage S3-compatible disponível
- ✅ Buckets criados e acessíveis
- ✅ Credenciais configuradas

### 6. Volumes Persistentes (se usar containers)
- ✅ Volumes persistentes configurados para `/tmp/*_downloads`
- ✅ Espaço suficiente para processar reports
- ✅ Permissões corretas nos diretórios

---

## 🎯 FOCO DO PROJETO

**Este projeto é especializado em:**

1. **ETL de Reports FOCUS**: Processo completo de extração, transformação e carga
2. **Multi-cloud**: Suporte nativo para AWS, OCI, GCP e Huawei
3. **Configurabilidade**: Tudo configurável via variáveis do Airflow
4. **Backup e Auditoria**: Preservação de arquivos raw para compliance
5. **Rastreabilidade**: Tracking de arquivos processados
6. **Logging Estruturado**: Logs detalhados para debugging

**Este projeto NÃO é:**

- ❌ Uma ferramenta de instalação/gerenciamento do Airflow
- ❌ Um conversor de dados nativos para FOCUS
- ❌ Uma solução de configuração de Data Exports
- ❌ Um sistema de gerenciamento de banco de dados

---

## 🚀 COMEÇANDO

1. Leia o [README.md](README.md) para visão geral
2. Siga o [QUICKSTART.md](QUICKSTART.md) para instalação
3. Entenda a [STRUCTURE.md](STRUCTURE.md) para arquitetura
4. Consulte [CONTRIBUTING.md](CONTRIBUTING.md) para desenvolvimento

---

## 📞 SUPORTE

Para questões sobre:
- **Este projeto**: Abra uma issue neste repositório
- **Apache Airflow**: Consulte a [documentação oficial](https://airflow.apache.org/docs/)
- **FOCUS Spec**: Consulte o [repositório FOCUS](https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec)
- **Provedores de nuvem**: Consulte a documentação específica de cada provedor

---

**Última atualização:** 2026-10-19
**Versão:** 1.0.0
