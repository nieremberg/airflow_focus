# Índice da Documentação

Este arquivo serve como índice central de toda a documentação do projeto Airflow FOCUS Pipeline.

## 📖 Documentação Principal

### [README.md](README.md)
**Visão geral completa do projeto**
- Badges de tecnologias utilizadas
- Descrição dos provedores suportados (AWS, OCI, GCP, Huawei)
- Explicação do Dell ECS como object storage on-premises
- Arquitetura e estrutura do projeto
- Fluxo de processamento das DAGs
- Configuração e instalação
- Uso e agendamento
- Formato dos dados FOCUS
- Monitoramento e troubleshooting
- Referências e links úteis

---

### [QUICKSTART.md](QUICKSTART.md)
**Guia de início rápido - Configure e execute em minutos**
- Pré-requisitos
- Instalação passo a passo
- Configuração do Airflow
- Configuração do PostgreSQL
- Configuração de credenciais dos provedores (AWS, OCI, GCP, Huawei)
- Configuração das variáveis do Airflow
- Inicialização e execução das DAGs
- Verificação da instalação
- Solução de problemas comuns

---

### [STRUCTURE.md](STRUCTURE.md)
**Estrutura detalhada do repositório**
- Árvore de diretórios completa
- Descrição de cada componente
- Explicação dos módulos (DAGs, loaders, storages, utils)
- Fluxo de dados com diagramas
- Padrões de design utilizados
- Guia de extensibilidade

---

### [CONTRIBUTING.md](CONTRIBUTING.md)
**Guia para contribuidores**
- Código de conduta
- Como reportar bugs
- Como sugerir melhorias
- Processo de Pull Request
- Guia de estilo de código (PEP 8)
- Convenções de naming
- Estrutura de testes
- Padrão de commits (Conventional Commits)
- Como adicionar um novo provedor (passo a passo completo)

---

### [CHANGELOG.md](CHANGELOG.md)
**Histórico de versões e mudanças**
- Versão 1.0.0 (2026-10-19)
- Features adicionadas
- Estrutura implementada
- Agendamento das DAGs
- Informações sobre backup storage
- Recursos de segurança
- Escopo e documentação

---

### [SCOPE.md](SCOPE.md)
**Escopo detalhado do projeto**
- O que ESTÁ incluído neste repositório
- O que NÃO está incluído (infraestrutura, data exports, geração FOCUS)
- Premissas do projeto
- Foco exclusivo no processo ETL
- Recursos úteis para cada tópico não coberto

---

## 📋 Arquivos de Configuração

### [requirements.txt](requirements.txt)
**Dependências Python do projeto**
- Apache Airflow
- Bibliotecas de processamento de dados (pandas, numpy, SQLAlchemy)
- SDKs dos provedores de nuvem (boto3, oci, google-cloud, esdk-obs-python)
- Utilitários

### [airflow_variables.example.json](airflow_variables.example.json)
**Template de variáveis do Airflow**
- Variáveis para buckets na nuvem (AWS, OCI, GCP, Huawei)
- Variáveis para Dell ECS (object storage on-premises)
- Comentários explicativos

### [.gitignore](.gitignore)
**Arquivos a serem ignorados pelo Git**
- Arquivos Python temporários
- Diretórios de ambiente virtual
- Credenciais e secrets
- Arquivos de dados
- Configurações locais

---

## 🔧 Código Fonte

### DAGs
- [dags/aws_focus_pipeline_dag.py](dags/aws_focus_pipeline_dag.py) - Pipeline AWS
- [dags/oci_focus_pipeline_dag.py](dags/oci_focus_pipeline_dag.py) - Pipeline OCI
- [dags/gcp_focus_pipeline_dag.py](dags/gcp_focus_pipeline_dag.py) - Pipeline GCP
- [dags/hpc_focus_pipeline_dag.py](dags/hpc_focus_pipeline_dag.py) - Pipeline Huawei

### Plugins
- [plugins/tasks.py](plugins/tasks.py) - Funções de tarefas principais

#### Loaders
- [plugins/loaders/base_loader.py](plugins/loaders/base_loader.py) - Classe base
- [plugins/loaders/loader_factory.py](plugins/loaders/loader_factory.py) - Factory
- [plugins/loaders/aws_loader.py](plugins/loaders/aws_loader.py) - Loader AWS
- [plugins/loaders/oci_loader.py](plugins/loaders/oci_loader.py) - Loader OCI
- [plugins/loaders/gcp_loader.py](plugins/loaders/gcp_loader.py) - Loader GCP
- [plugins/loaders/hpc_loader.py](plugins/loaders/hpc_loader.py) - Loader Huawei

#### Storages
- [plugins/storages/storage_factory.py](plugins/storages/storage_factory.py) - Factory
- [plugins/storages/aws_storage.py](plugins/storages/aws_storage.py) - Storage AWS S3
- [plugins/storages/oci_storage.py](plugins/storages/oci_storage.py) - Storage OCI
- [plugins/storages/gcp_storage.py](plugins/storages/gcp_storage.py) - Storage GCP
- [plugins/storages/hpc_storage.py](plugins/storages/hpc_storage.py) - Storage Huawei
- [plugins/storages/ecs_storage.py](plugins/storages/ecs_storage.py) - Storage para Backup (S3-compatible)

#### Utils
- [plugins/utils/files.py](plugins/utils/files.py) - Manipulação de arquivos
- [plugins/utils/database.py](plugins/utils/database.py) - Operações de banco
- [plugins/utils/dataframe.py](plugins/utils/dataframe.py) - Transformações
- [plugins/utils/logging_config.py](plugins/utils/logging_config.py) - Logging
- [plugins/utils/dag_callbacks.py](plugins/utils/dag_callbacks.py) - Callbacks

#### Interfaces
- [plugins/interfaces/storage_interface.py](plugins/interfaces/storage_interface.py) - Interface abstrata

---

## 🎯 Fluxo de Leitura Recomendado

### Para Usuários Iniciantes
1. Leia o [README.md](README.md) para entender o projeto
2. Siga o [QUICKSTART.md](QUICKSTART.md) para instalação
3. Consulte [STRUCTURE.md](STRUCTURE.md) se quiser entender a arquitetura

### Para Desenvolvedores
1. Leia o [README.md](README.md) para contexto
2. Estude [STRUCTURE.md](STRUCTURE.md) para entender a arquitetura
3. Leia [CONTRIBUTING.md](CONTRIBUTING.md) antes de contribuir
4. Revise o código fonte nos diretórios `dags/` e `plugins/`

### Para Administradores
1. Leia o [README.md](README.md) para visão geral
2. Siga o [QUICKSTART.md](QUICKSTART.md) para configuração
3. Configure variáveis usando [airflow_variables.example.json](airflow_variables.example.json)
4. Consulte seção de Troubleshooting no [README.md](README.md)

---

## 🔗 Links Externos Úteis

### Tecnologias
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Python Documentation](https://docs.python.org/3/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

### Cloud Providers
- [AWS Cost and Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/)
- [Oracle Cloud Cost Reports](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/usagereportsoverview.htm)
- [GCP Cloud Billing Export](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- [Huawei Cloud Cost Center](https://support.huaweicloud.com/intl/en-us/usermanual-billing/)

### Dell ECS
- [Dell ECS Overview](https://www.dell.com/en-us/dt/storage/ecs/index.htm)

### FinOps e FOCUS
- [FinOps Foundation](https://www.finops.org/)
- [FOCUS Specification](https://focus.finops.org/)
- [FOCUS GitHub Repository](https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec)

---

## 📝 Notas

- Todos os arquivos de documentação estão em português brasileiro
- Os exemplos de código seguem PEP 8
- A documentação segue o formato Markdown (CommonMark)
- Links relativos são usados para navegação interna
- Links absolutos são usados para recursos externos

---

**Última atualização:** 2026-10-19
**Versão:** 1.0.0
