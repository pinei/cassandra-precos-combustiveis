# Cassandra Preços Combustíveis

Requer que o Apache Cassandra seja executado em outro container de nome "cassandra" na rede "cassandra-network".

## SQL vs NoSQL

Características dos bancos de dados SQL:

- Bases de dados relacionais (RDBMS) (tabelas relacionadas por campos chave)
- São escaláveis verticalmente, com upgrade dos nós (CPU, memória)
- Usam a linguagem de domínio SQL (Structured Query Language) como padrão
- Os dados tem esquema pré-definido
- Bom suporte ao conceito de transação e consultas de dados complexas
- Seguem as propriedades ACID (atomicidade, consistência, isolamento, durabilidade) no gerenciamento de transações
- Estruturas de dados normalizadas são encorajadas para garantir integridade, qualidade dos dados e eficiência no armazenamento

Casos de uso:

- Sistemas transacionais orientadas ao usuário com tráfego de pequenos volumes de dados
- Ambientes com tráfego de grande volume de dados em lote, permitindo consultas complexas, tais como data warehouses

Exemplos:

- MySQL, PostgreSQL, Oracle, SQL Server, Microsoft SQL Server

NoSQL pode ser entendido como Not Only SQL. É um movimento de tecnologias que seguem paradigmas diferentes dos bancos relacionais. A ideia não é substituir os bancos relacionais e sim somar outras alternativas de armazenamento, se tornando mais uma opção para os desenvolvedores.

Características dos bancos de dados NoSQL:

- Bases de dados não relacionais, onde os dados podem ser estruturados em documentos, chave-valor, grafos, ou formato colunar
- São escaláveis horizontalmente, com adição de mais nós de processamento
- Infraestrutura mais elástica, podendo escalar pra cima ou pra baixo com mais facilidade para acomodar as mudanças de uso ou volume de dados
- Os dados tem esquemas flexíveis e dinâmicos ou são não estruturados
- Usam linguagens de domínio específicas, algumas vezes semelhantes ao SQL
- Não necessariamente seguem as propriedades ACID (atomicidade, consistência, isolamento, durabilidade) na leitura e escrita de dados
- Geralmente faz uso de estruturas de dados não normalizadas em favor de desempenho na escrita e/ou leitura de dados

Casos de uso:

- Aplicações com requisitos de grande volume de dados e respostas rápidas como dashboards, catálogos, mapas e redes sociais
- Cache de dados
- Logs

Exemplos:

- MongoDB, BigTable, Cassandra, CouchDB, Amazon DynamoDB, Redis, ElasticSearch, HBase, Neo4j

## Cassandra

O Apache Cassandra é um banco de dados do tipo não relacional (NoSQL) e colunar.

Criado originalmente pelo Facebook, com arquitetura inspirada pelo DynamoDB da Amazon e modelo de dados baseado no BigTable do Google. Evolui como open source desde 2008.

No contexto do teorema CAP (disponibilidade, consistência, tolerância a partição) O Cassandra é considerado um banco AP (disponibilidade, tolerância a partição) tendo como principais características a alta escalabilidade e distribuição dos nós de processamento. A leitura dos dados pode não ter consistência, considerando que o Cassandra opera com mecanismos de reorganização frequente do cluster.

## Iniciando o Cassandra via Docker e acessando a console

Optamos por usar a instalação do Cassandra via Docker, pela facilidade de inicialização e manutenção para as finalidades deste projeto.

Criamos uma rede do Docker nomeada `cassandra-network` para que outros containters possam interagir com o serviço do Cassandra.

Executar no host:

```sh
$ docker network create cassandra-network
$ docker run --name cassandra --network cassandra-network cassandra:4.1.3
```

Uma vez iniciado o serviço do Cassandra, podemos assumir a shell do container e iniciar a console de comandos para o Cassandra (cqlsh)

https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html

```sh
$ docker exec -it cassandra /bin/bash
$ cqlsh
Connected to Test Cluster at 127.0.0.1:9042
[cqlsh 6.1.0 | Cassandra 4.1.3 | CQL spec 3.4.6 | Native protocol v5]
Use HELP for help.
cqlsh> 
```

Para parar e reiniciar o contaner:

```sh
$ docker stop cassandra
$ docker start cassandra
$ docker attach cassandra
```

## Base de dados para o projeto com Cassandra

Optamos por usar os dados de "Série Histórica de Preços de Combustíveis e de GLP" catalogado no Portal de Dados Abertos do Governo Federal

https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp

Temos dados brutos de 2004 a 2023 em formato CSV.

https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis

Como alternativa, existe o projeto "Base dos Dados" que oferece infraestrutura BigTable com tabelas tratadas para acessar os dados da ANP (e muitos outros).

https://basedosdados.org/dataset/6ea3e28a-42be-401a-a066-ad87ca931e69?table=3a7cb29a-0bdf-4f44-bab1-d27872e565ff

Vamos usar esses dados.

## Ambiente de desenvolvimento

Esse projeto usa o Visual Studio Code com `devcontainer` para execução do ambiente de desenvolvimento em container Docker usando uma imagem com Python 3 e Spark instalado como `feature`.

Ref.: https://code.visualstudio.com/docs/devcontainers/containers

Após inicialização do `devcontainer` fizemos a instalação do PySpark para interação com o Spark

```sh
pip install pyspark
```

O Visual Studio Code tem suporte ao conceito de notebook, como os que rodam no Jupyter. São arquivos de extensão `.ipynp`. A interface de notebook permite execução de código Python de forma interativa e documental.

As interação com o `pip` foram feitas via terminal, então não estão documentadas no(s) notebook(s).

## Carga de dados para ingestão

Usamos o notebook `ingestao.ipynb` para o processo de ingestão de dados no Cassandra.

1. Instalação da `basedosdados` no ambiente python

```sh
pip install basedosdados
```

2. Atualização de dependências. Foi necessário porque notamos incompatibilidade no uso de versão antiga do client para acessar o serviço do BigQuery.

```sh
pip install -U google-cloud-bigquery google-api-core google-cloud-bigquery-storage google-cloud-storage db-dtypes
```

3. Carga dos dados do BigQuery para um dataframe do Pandas

```py
import basedosdados as bd

df = bd.read_table('br_anp_precos_combustiveis', 'microdados', billing_project_id=BILLING_PROJECT_ID, limit=None)
```

A carga completa dos dados ocorre em cerca de 20 minutos. O dataframe carregado contem cerca de 14 milhões de observações e ocupa aproximadamente 9 GB em memória.

```py
print(f'Observações: {len(df)}')
print(f'Memória: {df.memory_usage(deep=True).sum() / 1048576} MB')
# Observações: 14123868
# Memória: 8850.27960395813 MB
```

Para testes com massa reduzida de dados, usar o parâmetro `limit`.

4. Schema dos dados

```py
df.info()
```

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1000 entries, 0 to 999
Data columns (total 14 columns):
 #   Column                Non-Null Count  Dtype         
---  ------                --------------  -----         
 0   ano                   1000 non-null   Int64         
 1   sigla_uf              1000 non-null   object        
 2   id_municipio          1000 non-null   object        
 3   bairro_revenda        1000 non-null   object        
 4   cep_revenda           1000 non-null   object        
 5   endereco_revenda      1000 non-null   object        
 6   cnpj_revenda          1000 non-null   object        
 7   nome_estabelecimento  1000 non-null   object        
 8   bandeira_revenda      1000 non-null   object        
 9   data_coleta           1000 non-null   datetime64[ns]
 10  produto               1000 non-null   object        
 11  unidade_medida        1000 non-null   object        
 12  preco_compra          219 non-null    float64       
 13  preco_venda           1000 non-null   float64       
dtypes: Int64(1), datetime64[ns](1), float64(2), object(10)
memory usage: 110.5+ KB
```

## Problema de negócio

Queremos fazer análise dos preços de combustíveis por `produto` e `sigla_uf`. A `data_coleta` é um campo natural para ordenação dos dados.

Nossa chave será formada pelos campos `produto`, `sigla_uf`, `data_coleta`, `cnpj_revenda`

## Criando o keyspace e tabela no Cassandra

Usando o `cqlsh` no container do Cassandra, criamos um keyspace `anp` para a nossa base de dados

```sql
cqlsh> CREATE KEYSPACE IF NOT EXISTS anp WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;
```

No keyspace já podemos criar a nossa tabela.

```sql
use anp ;

CREATE TABLE IF NOT EXISTS precos_combustiveis_por_produto_e_uf (
  	ano smallint,
    sigla_uf text,
    id_municipio int,
    bairro_revenda text,
    cep_revenda text,
    endereco_revenda text,
    cnpj_revenda text,
    nome_estabelecimento text,
    bandeira_revenda text,
    data_coleta date,
    produto text,
    unidade_medida text,
    preco_compra decimal,
    preco_venda decimal,
  	PRIMARY KEY ((produto, sigla_uf), data_coleta, cnpj_revenda)
) WITH CLUSTERING ORDER BY (data_coleta ASC) ;
```

## Ingestão dos dados

Dado que já temos o dataframe `df` com os dados em memória, podemos fazer a ingestão usando a API do PySpark com conector do Cassandra

Configurando a sessão do Spark:

```py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Crie uma sessão do Spark
spark = ( 
    SparkSession.builder.appName("Cassandra")
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    .getOrCreate()
)
```

Fazendo a conversão para dataframe do Spark e salvando na base do Cassandra:

```py
df_spark = spark.createDataFrame(df)

df_spark.write.format("org.apache.spark.sql.cassandra")
.options(**CASSANDRA_CONF)
.options(keyspace="anp", table="precos_combustiveis_por_produto_e_uf")
.mode("append")
.save()
```

Houve problema de OutOfMemory da JVM em uma máquina com 16GB de RAM. A solução foi adaptar o código para o uso de funções parametrizadas pelo campo `ano` para se trabalhar com dataframes menores. Foi possivel rodar a ingestão ano a ano (2004 a 2023) conforme evidenciado no notebook.


## Referências

- [ANÁLISE DE MICRODADOS DE COMBUSTÍVEIS NO BRASIL](https://analisemacro.com.br/data-science/analise-de-microdados-de-combustiveis-no-brasil/)
- [Cassandra and PySpark](https://medium.com/@yoke_techworks/cassandra-and-pyspark-5d7830512f19)
- [Data Operations with Spark and Cassandra](https://anant.us/blog/modern-business/data-operations-with-spark-and-cassandra/)
- [Apache Cassandra: confira absolutamente tudo a respeito](https://medium.com/nstech/apache-cassandra-8250e9f30942)