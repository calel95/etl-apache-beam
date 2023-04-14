import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import *
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = ['id','data_iniSE','casos','ibge_code','cidade','uf','cep','latitude','longitude']

def txt_for_list(element, delimiter='|'):
    """Recebe um texto e retorna uma lista de elementos pelo delimitador"""
    return element.split(delimiter)
def list_for_dictionary(element, colunas):
    """passa a lista para dicionario"""
    return dict(zip(colunas,element))
def data_transformation(element):
    """Recebe um dicionário e cria um novo campo com ano-mes, retorna o mesmo dicionário com o novo campo"""
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element
def uf_pk(element):
    """recebe um dicionario e retorna uma tupla com o UF e o elemento (UF, dicionario)"""
    chave = element['uf']
    return (chave, element)

# def casos_dengue(element):
#     """Recebe uma tupla ('RS' [{}, {}, ...])
#         e retorna uma tupla ('RS-2020-04', 8.0)"""
#     uf, registros = element
#     for registro in registros:
#         if bool(re.search(r'\d', registro['casos'])):
#             yield (uf, f"-{registro['ano_mes']}", float(registro['casos']))
#         else:
#             yield (uf, f"-{registro['ano_mes']}", 0.0)

def casos_dengue(element):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retornar uma tupla ('RS-2014-12', 8.0)
    """
    uf, registros = element
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_mes_chuva(element):
    """Recebe uma lista de elementos e retorna uma tupla com chave e valor em mm, igual em casos_dengue"""
    data, mm, uf = element
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f"{uf}-{ano_mes}"
    if float(mm) <0:
        mm = 0
    else:
        mm = float(mm)
    return chave, mm

def arredonda(element):
    chave, mm = element
    return chave, round(mm)

def remove_campos_vazios(element):
    pk, dados = element

    if all([dados['chuvas'],dados['dengue']]):
        return True
    return False

def descompactar(element):
    pk, dic = element
    chuvas = dic['chuvas'][0]
    dengue = dic['dengue'][0]
    uf, ano, mes = pk.split("-")
    return uf,ano,mes,str(chuvas),str(dengue)

def prepara_csv(element, delimiter=';'):
    return f'{delimiter}'.join(element)

#trata  o dataframe de dengue
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >>
        ReadFromText('datas/sample_casos_dengue.txt', skip_header_lines=1)
    | "Passa o arquivo original de txt para lista" >> beam.Map(lambda a:a.split("|"))
    #| "Passa o arquivo original de txt para lista" >> beam.Map(txt_for_list)
    | "Passa de lista para dicionario" >> beam.Map(lambda a,b: dict(zip(b,a)),colunas_dengue)
    #| "Passa de lista para dicionario" >> beam.Map(list_for_dictionary, colunas_dengue)
    #| "Cria o campo ano_mes" >> beam.Map(data_transformation)
    | "Cria o campo ano_mes" >> beam.Map(lambda a: {'ano_mes': '-'.join(a['data_iniSE'].split('-')[:2]), **a})
    #| "Cria chave pelo estado uf" >> beam.Map(uf_pk)
    | "Cria chave pelo estado uf" >> beam.Map(lambda a: (a['uf'], a))
    | "agrupando os registros por uf" >> beam.GroupByKey()
    | "cria a chave ESTADO-ANO_MES" >> beam.FlatMap(casos_dengue)
    #| "cria a chave ESTADO-ANO_MES" >> beam.FlatMap(lambda element: [(f"{element[0]}-{r['ano_mes']}", float(r['casos'])) for r in element[1] if r['casos'] is not None])
    | "soma o casos da chaves" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)
)

#trata o dataframe de chuvas
chuvas = (
        pipeline
        | "Leitura do dataset de chuvas" >>
        ReadFromText('datas/sample_chuvas.csv', skip_header_lines=1)
        | "Passa o arquivo original de chuva.txt para lista" >> beam.Map(lambda a: a.split(","))
        | "cria a chave ESTADO-ANO_MES de chuvas" >> beam.Map(chave_uf_mes_chuva)
        | "soma o casos da chuva da chaves" >> beam.CombinePerKey(sum)
        #| "arredonda valor de mm" >> beam.Map(arredonda)
        | "arredonda valor de mm" >> beam.Map(lambda a: (a[0], round(a[1],2)))
        #| "Mostrar resultados" >> beam.Map(print)

)

#junta os dois dataframes tratados

resultado = (
    ({'chuvas': chuvas, 'dengue': dengue})
    |'mesclando as duas tabelas' >> beam.CoGroupByKey()
    |'Elimina os campos vazios' >> beam.Filter(remove_campos_vazios)
    | 'descompactar a tupla' >> beam.Map(descompactar)
    |'prepara CSV, passa o registro pra string' >> beam.Map(prepara_csv)
    #| beam.Flatten()
    #| "agrupa as collections" >> beam.GroupByKey()
    #| "Mostrar resultados final" >> beam.Map(print)

)

resultado | "Cria o arquivo" >> WriteToText('ref/resultado.csv', file_name_suffix='csv',header='uf;ano;mes;chuva_mm;casos_dengue')
pipeline.run()