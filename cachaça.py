#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reestruturado on Wed Aug 18 23:02:29 2021

@author: danieln
"""

version = 2.0

# Importações
# -----------

from urllib.request import urlopen, HTTPError
from time import sleep
import pandas as pd
import schedule
import datetime


# Classe
# ------

class Escavador(object):
    """TODO
    """
    
    def __init__(self, csv_path, data=datetime.date.today(), salvar=True, pesquisar_preços_agora=True):
        """Estruturas base."""
        self.df = pd.read_csv(csv_path)
        
        self.data = data
        self.csv_path = csv_path
        
        if pesquisar_preços_agora:
            self.atualiza_preços(salvar=salvar, verbose=True)
        else:
            self.preços_agora = None
    
    def cabeça(self):
        """Retorna um dataframe com as duas primeiras colunas (nome e url das cachaças).
        """
        return self.df.iloc[:,0:2]
    
    def coração(self):
        """Retorna um dataframe com somente os preços indexados pelas datas.
        """
        return self.df.iloc[:,2:]
    
    def atualiza_data(self, data=datetime.date.today()):
        """Atualiza a data do Escavador para a data indicada.
        O padrão é o dia de hoje.
        """
        self.data = data
        return self.data
    
    def atualiza_preços(self, salvar=True, verbose=True):
        """Atualiza os preços das cachaças.
        Se verbose, mostra os preços conforme forem obtidos.
        """
        
        def pega_preço(url, x=0) -> float:
            """Pega o primeiro preço a partir da posição x no html do url.
            O url é só DEPOIS da barra do .com.br/
            """
            url = 'https://www.cachacarianacional.com.br/' + url
            
            with urlopen(url) as f:
                pagina = f.read().decode('utf-8')
            
            z = pagina.find("skuBestPrice", x) # Encontramos onde está isso para podermos retirar e ficar só com o que interessa
            y = pagina.find("<", z+1) # É onde acaba a parte interessante
            
            real = pagina[z+17:y-3] # Usamos o slice para pegar solo el necessario
            centavos = pagina[z+20:y]
            return float(real) + float(centavos)/100
        
        if verbose:
            ml = max([len(x) for x in self.cabeça()['CACHAÇA']]) 
    
        value = {}
        for nome, url in zip(self.cabeça()['CACHAÇA'], self.cabeça()['URLS']):
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
        
        self.preços_agora = value
        if salvar: self.sarava()
        return value
    
    def filtro_repetidas(self, salvar=True):
        """Checa se há colunas idênticas no dataframe e as deleta, caso haja.
        Modifica o objeto automaticamente.
        Salva as alterações no csv se indicado.
        """
        cols = [col for col in self.df][2:] # cortando as cols de nome e links
        repetidas = []
        for i, col in enumerate(cols):
            if col is cols[-1]: # se é a última coluna, encerramos por aqui
                pass
            else: # compara com a seguinte e checa se é idêntica
                col1, col2 = col, cols[i+1]
                comparação = self.df[col1] == self.df[col2]
                if comparação.all(): # Se os elementos são TODOS iguais
                    repetidas.append( col2 ) # deleta a coluna mais recente
        
        for col in repetidas: 
            del self.df[col]
    
        # sarava
        if salvar:
            self.df.to_csv(self.csv_path, index=False)
        
        return self.df
        
    def sarava(self):
        """Salva o dicionário das cachaças associados ao preço no arquivo csv no path indicado.
        
        Retorna
        -------
        0 : Tudo certo
        1 : As cachaças não são as mesmas ou a ordem está trocada (o correto é sempre a ordem alfabética)
        """
        self.filtro_repetidas(salvar=False)
        
        katias = list(self.df['CACHAÇA']) # Cachaças já salvas
        novas  = list(self.preços_agora.keys()) # Cachaças a serem salvas
        if katias == novas: # Se são as mesmas, tudo bem
            self.df[self.data] = list(self.preços_agora.values())
            self.df.to_csv(self.csv_path, index=False)
            return 0
        else: # TODO: lidar com isso
            raise NotImplementedError("Opa! As cachaças não são as mesmas. Assim não dá né!")
            return 1
    
    def autoatualização(self, verbose=True):
        """Pesquisa preços e registra todo dia ao meio dia.
        """
        def job(verbose):
            if verbose:
                print("Vasculhando os preços...")
            self.atualiza_preços(verbose=verbose)
            if verbose:
                print("Salvando os resultados...")
            self.sarava()
            print("Preços atualizados!\n\n")
        
        job(verbose)
        schedule.every().day.at("12:00").do(job)
        while True:
            schedule.run_pending()
            sleep(1)

# Tarefas
# -------
if __name__ == '__main__':
    escavador = Escavador('katia.csv', pesquisar_preços_agora=False)
    
    #TODO: colocar argv pro verbose
    escavador.autoatualização(verbose=True)
