# project Sume 

## intutito do projeto
esse projeto tem o obijetivo de trabalhar com dados publicos do poder legislativo brasileiro de maneira a criar uma perspectiva maior sobre esse poder 

### sume 
Sumé (também conhecido como Zumé, Pay Sumé ou Tumé, entre outros nomes) é a denominação de uma antiga entidade da mitologia dos povos tupis do Brasil cuja descrição variava de tribo para tribo. Tal entidade teria estado entre os índios antes da chegada dos portugueses e teria transmitido a eles uma série de conhecimentos, como a agricultura, o fogo e a organização social. 

[fonte](https://pt.wikipedia.org/wiki/Sum%C3%A9)


### Estrutura da pasta
```
├── README.md                        <- The top-level README for setting expectations and summarizing the conclusion for this analysis.
│
├── analises                         <- analises folder
│
│
├── pipelines                        <- data acquisition/transformation folder
│
│
├── src                              <- Reusable Python code
│   ├── __init__.py                  <- Makes src a Python module
│   ├── config.py                    <- config file
│   ├── paths.py                     <- paths file
│   ├── .env                         <- environment variables file
│
├── environment.yml                  <- The conda env file for reproducing the environment
│
└── setup.py                         <- makes 'src' installable so it can be imported
```



## Como rodar os scripts 

### environment 
Esse projeto funciona com base em um environment conda para a gestão das bibliotecas utilizadas e da pasta src que contem as fonctions que permeiam o projeto. 

para rodar o environment voce precisa de uma instalação anaconda e seguir os seguintes passos: 
1) navegar pelo pronpt de comando até a pasta do projeto
2) e rodar os comandos 

``` conda env create -f environment.yml```

``` conda activate project-sume```

isso instalara as bibliotecas com as versões definidas no .yml e adiciona os arquivos da pasta src como uma "biblioteca" no environment

3) e criar o kernel no jupyter notebook

```conda install ipykernel```

```ipython kernel install --user --name=project-sume```

### .env
Para armazenar os dados de conexão estamos utilizando um arquivo .env, como esse arquivo contem informações sensíveis de acesso esse arquivo não sobe para o github. Para replicar essa conexão você necessita fazer os seguintes passos: 
1) navegar até a pasta src
2) rodar: 
```nano .env```
3) inserir no arquivo as seguintes variáveis 

    host='seu host'

    port='sua porta'

    username='seu user'

    password='sua senha'

Com isso as funções de conexção já realizaram a conexão com o seu banco de dados. 

