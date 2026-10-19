# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [1.0.0] - 2026-10-19

### Adicionado
- Pipeline FOCUS para AWS com suporte a arquivos .gz comprimidos
- Pipeline FOCUS para OCI com suporte a arquivos .gz comprimidos
- Pipeline FOCUS para GCP com suporte a arquivos .csv
- Pipeline FOCUS para Huawei Cloud com suporte a arquivos .zip comprimidos
- Sistema de tracking de arquivos processados para evitar duplicação
- Sincronização automática com Dell ECS (object storage on-premises) para armazenar reports raw
- Armazenamento de arquivos originais no Dell ECS criando data lake on-premises
- Logging estruturado com métricas detalhadas
- Callbacks configuráveis para sucesso e falha das DAGs
- Factories para loaders e storages facilitando extensibilidade
- Limpeza automática de diretórios temporários
- Documentação completa (README, QUICKSTART, STRUCTURE, CONTRIBUTING)
- Badges de tecnologias no README
- Arquivo de exemplo de variáveis do Airflow com comentários
- Suporte a múltiplos formatos de compressão (.gz, .zip)
- Retry automático com backoff exponencial
- Paralelização de tasks quando possível

### Estrutura
- `dags/`: DAGs do Airflow para cada provedor
- `plugins/tasks.py`: Funções de tarefas reutilizáveis
- `plugins/loaders/`: Loaders específicos por provedor
- `plugins/storages/`: Integrações com storage
- `plugins/utils/`: Utilitários (files, database, logging, callbacks)
- `plugins/interfaces/`: Interfaces e contratos

### Agendamento
- AWS: 4x ao dia (10h, 16h, 22h, 4h UTC)
- OCI: A cada 2 horas
- GCP: Diariamente às 10h UTC
- Huawei: Diariamente às 6h UTC

### Backup Storage para Auditoria
- Suporte a qualquer object storage compatível com S3
- Opções: Dell ECS, MinIO (on-premises), AWS S3, Wasabi, Backblaze B2 (cloud)
- Armazena reports raw baixados dos buckets na nuvem
- Mantém arquivos originais (.gz, .zip, .csv) sem transformação
- Organização por provedor: raw/aws/, raw/oci/, raw/gcp/, raw/huawei/
- Serve como data lake para auditoria e controle corporativo dos dados
- Permite reprocessamento a partir dos arquivos originais
- Variáveis configuráveis: BACKUP_STORAGE_TYPE, BACKUP_ENDPOINT_URL, credenciais

### Variáveis do Airflow
- Credenciais dos provedores (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc)
- Configurações de bucket e prefix para cada provedor
- Configurações do backup storage (tipo, endpoint, credenciais, prefixes)
- Configurações do banco de dados (host, porta, database, usuário, senha)
- Nomes das tabelas FOCUS (TABLE_AWS_FOCUS, TABLE_OCI_FOCUS, etc)
- Suporte a import/export via JSON (airflow_variables.example.json)
- Nenhuma configuração hard-coded no código

### Escopo e Documentação
- Documento SCOPE.md detalhando o que está e o que NÃO está incluído
- Seção "Escopo e Premissas" no README
- Documentação sobre volumes persistentes para containers/Kubernetes
- Exemplos de configuração Kubernetes e Docker Compose
- Esclarecimento: Data Exports e geração FOCUS não são abordados
- Premissa: Reports FOCUS já existem nos buckets dos provedores
- Foco exclusivo no processo ETL dos reports FOCUS

### Volumes Persistentes
- Documentação sobre necessidade de volumes para containers/Kubernetes
- Tabela com diretórios temporários por DAG
- Exemplos de PersistentVolumeClaim para Kubernetes
- Exemplos de volumes para Docker Compose
- Recomendações de dimensionamento (50-100GB por provedor)

### Segurança
- Credenciais gerenciadas via variáveis de ambiente e arquivos de configuração
- Senhas e chaves não versionadas (.gitignore configurado)
- Suporte a diferentes métodos de autenticação por provedor
- Backup storage garante que dados de billing ficam sob controle corporativo
- Nenhuma senha ou credencial hard-coded no código

[1.0.0]: https://github.com/nieremberg/airflow_focus/releases/tag/v1.0.0
