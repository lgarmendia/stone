
# Desafio case data engineer

Este projeto automatiza o pipeline de dados utilizando Apache Airflow e PySpark para processar os dados por meio de três camadas principais: Bronze, Silver e Gold. O objetivo é extrair, transformar e carregar (ETL) os dados, refinando-os até atingir uma forma de alta qualidade e inserir os dados em banco de dados para analises.

# Escopo do projeto
Este projeto demonstra o uso do Apache Airflow "Dockerizado" para o agendamento e monitoramento de tarefas ETL que executam scripts em PySpark. Ele transforma dados relacionados a empresas e socios através de diferentes estágios (Bronze, Prata e Ouro) aplicando transformações incrementais e apos salva os dados no formato delta e também no Duckdb.
A arquitetura do projeto segue a abordagem Medallion (Bronze-Prata-Ouro).


### Motivos de escolha do Duckdb
#### Arquitetura Embarcada (Embedded)
* Funciona como uma biblioteca (sem servidor)
* Zero configuração ou infraestrutura adicional
* Ideal para projetos simples e autônomos

#### Desempenho Analítico  
* Processamento columnar nativo (OLAP)
* Consultas rápidas em datasets de sócios/empresas (JOINs, GROUP BY)
* Paralelismo automático 

#### Integração com ETL em Python

#### Eficiência em Memória
* Dados processados na RAM (ideal para análises intermediária

# Estrutura do projeto

``` stone/
├── dags/                        # Airflow DAGs (pipelines) 
├── data/                        # Dados do projeto para processamento
│   ├── process/                 # Dados processados
├── duckdb/
│   ├── silver/                  # Camada silver duckdb
│   ├── gold/                    # Camada gold duckdb
├── src/
│   ├── bronze/                  # Camada bronze dos scripts
│   ├── silver/                  # Camada silver dos scripts
│   ├── gold/                    # Camada gold dos scripts
│   └── utils/                   # Utility scripts
├── docker-compose.yml           # Configuração Docker Compose 
├── Dockerfile                   # Dockerfile para configurar o ambiente
├── Makefile                     # Comandos de automação para construção do ambiente
├── README.md                    # Documentação do projeto
├── arquittura.png               # Desenho da arquitetura
└── requirements.txt             # Dependências do Python 
```

# Primeiros Passos

###  Pré requisitos

Para rodar este projeto, você precisa ter os seguintes itens instalados:

* Docker: Guia de instalação
* Make: Guia de instalação (para usuários Windows)


## Instalação do Docker Desktop  

### 1. Baixar o Docker Desktop  
Acesse o site oficial do Docker e faça o download do instalador:  
[Download Docker Desktop](https://www.docker.com/products/docker-desktop/)  

### 2. Instalar o Docker Desktop  
Após o download, abra o arquivo `.exe` baixado para iniciar o processo de instalação.  

### 3. Configurar a instalação  
Na janela do instalador do Docker Desktop, marque a opção **"Install required components"** para instalar o backend WSL 2 e outros componentes necessários.  

### 4. Finalizar a instalação  
Clique em **"OK"** para prosseguir e aguarde a conclusão da instalação.  

## Instalação do Make utilizando Git Bash (Para usuários Windows)  

### 1. Instalar o Git para Windows  
- Faça o download e instale o **Git para Windows** a partir do site oficial:  
  [Download Git para Windows](https://gitforwindows.org/)  
- Durante o processo de instalação, **certifique-se de marcar a opção "Git Bash"** para que ele seja instalado.  

### 2. Adicionar o Make ao Git Bash  

#### Baixar o Make para Windows  
- Baixe a versão independente do **Make para Windows**:  
  [Download Make para Windows](https://sourceforge.net/projects/ezwinports/files/)  
- Escolha a versão **`make-4.4.1-without-guile-w32-bin.zip`** (ou superior), **mas que seja "without guile"**.  

#### Instalar o Make  
1. Extraia o conteúdo do arquivo `.zip` baixado.  
2. Copie o arquivo `bin/make.exe`.  
3. Navegue até a pasta de instalação do Git para Windows, geralmente localizada em:  
   ```plaintext
   C:\Program Files\Git\usr\bin

## Instruções de Configuração  

### 1. Clonar o repositório  
- Execute o seguinte comando para clonar o repositório e acessar a pasta do projeto:  
```sh
git clone https://github.com/lgarmendia/stone.git
cd stone
```
### 2. Construir as imagens Docker  
- Para construir as imagens Docker, execute:  
``` 
make build-image
```
### 3. Iniciar o ambiente usando Docker Compose  
- Para iniciar o ambiente com Docker Compose, execute:  
```
make image-up
```

## Configuração do Docker  

###  Docker Compose
O projeto utiliza Docker Compose para configurar o ambiente.
Os seguintes serviços são definidos:

* Spark Master e Worker: Utilizados para executar jobs distribuídos em PySpark.
*  Airflow: Utilizado para orquestrar o pipeline de dados.

### Executando o Pipeline
Após iniciar o ambiente, você pode acionar o pipeline manualmente pela interface do Airflow.

### Acessar o Airflow:
 1. Abra o navegador e vá para: http://localhost:8080
 2. Use as credenciais padrão: admin/admin
 3. No Airflow UI, acione a DAG "stone_pipeline".
