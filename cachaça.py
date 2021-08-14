# UTF-8

# Importações
# -----------

from urllib.request import urlopen, HTTPError
from datetime import date
from time import sleep
import pandas as pd
import schedule


# Variáveis
# ---------

df_cachacas = pd.read_csv('katia.csv')

# trabalharemos sempre com o nome da cachaça
nomes, urls = list(df_cachacas['CACHAÇA']), list(df_cachacas['URLS'])
cachacas = {nome: url for nome, url in zip(nomes, urls)}


# Funções
# -------

def hoje() -> str:
    """Retorna uma string com a data de hoje.
    
    Exemplo
    -------
    >>> hoje()
    '11-02-2020'
    """
    return f'{date.today().day}-{date.today().month}-{date.today().year}'

def pega_preço(url, x=0, cachaçaria_nacional=True) -> float:
    """Pega o primeiro preço a partir da posição x no html do url.
    """
    if cachaçaria_nacional:
        url = 'https://www.cachacarianacional.com.br/' + url
        
    with urlopen(url) as f:
        pagina = f.read().decode('utf-8')
    
    z = pagina.find("skuBestPrice", x) # Encontramos onde está isso para podermos retirar e ficar só com o que interessa
    y = pagina.find("<", z+1) # É onde acaba a parte interessante
    
    real = pagina[z+17:y-3] # Usamos o slice para pegar solo el necessario
    centavos = pagina[z+20:y]
    return float(real) + float(centavos)/100

def pega_todos_os_preços(dicionario_de_cachaças: dict, verbose=False) -> dict:
    """Recebe um dicionário associando a cachaça ao URL e retorna um dicionário associando o mesmo índice ao seu preço atual.
    """
    if verbose:
        ml = max([len(x) for x in dicionario_de_cachaças.keys()])

    value = {}
    for nome, url in dicionario_de_cachaças.items():
        try:
            preço = pega_preço(url)
            value[nome] = preço
            if verbose: print(f" > {nome:<{ml}} custando R$ {preço}")
        except ValueError:
            value[nome] = 0  # Cachaça esgotada!
            if verbose: print(f" > {nome:<{ml}} esgotada!")
        except HTTPError:
            value[nome] = -1 # Link quebrado!
            if verbose: print(f" > {nome:<{ml}} com link quebrado!")
    return value

# Agora a parte de salvar e abrir o trabalho

def sarava(preços:dict, path:str='katia.csv', dia=hoje()):
    """Salva o dicionário das cachaças associados ao preço no arquivo csv no path indicado.
    
    Retorna
    -------
    0 : Tudo certo
    1 : As cachaças não são as mesmas ou a ordem está trocada (o correto é sempre a ordem alfabética)
    """
    df = pd.read_csv(path)
    katias = list(df['CACHAÇA']) # Cachaças já salvas
    novas  = list(preços.keys()) # Cachaças a serem salvas
    if katias == novas: # Se são as mesmas, tudo bem
        df[dia] = list(preços.values())
        df.to_csv(path, index=False)
        return 0
    else: # TODO: lidar com isso
        print("Opa! As cachaças não são as mesmas. Assim não dá né!")
        return 1


# Tarefas
# -------
if __name__ == '__main__':
    def job(verbose=False):
        if verbose:
            print("Vasculhando os preços...")
        p_agora = pega_todos_os_preços(cachacas, verbose=verbose)
        if verbose:
            print("Salvando os resultados...")
        sarava(p_agora, dia=hoje())
        print("Preços atualizados!\n")
    
    #TODO: colocar argv
    verbose = True

    job(verbose)
    schedule.every().day.at("12:00").do(job)
    while True:
        schedule.run_pending()
        sleep(1)
