from datetime import datetime, timedelta
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

from dotenv import load_dotenv

load_dotenv()
# from webdriver_manager.microsoft import EdgeChromiumDriverManager
# from selenium.webdriver.edge.service import Service
# from selenium.webdriver.edge.options import Options

import queue
import numpy as np
import threading
import time
import requests
import os
 
def limpa_arquivos():
    arquivos = os.listdir("./files")
    licitacoes = [arquivo for arquivo in arquivos if "licitacoes" in arquivo]
    produtos = [arquivo for arquivo in arquivos if "produtos" in arquivo]
 
    licitacoes_df = []
    for path in licitacoes:
        caminho = f"./files/{path}"
        try:
            licitacao = pd.read_excel(caminho)
        except Exception as e:
            print("erro no: ", caminho)
        licitacoes_df.append(licitacao)
    for path in licitacoes:
        if 'all' not in path:
            os.remove(f"./files/{path}")
    licitacoes_concat = pd.concat(licitacoes_df, ignore_index=True)
    licitacoes_concat.to_excel(f"./files/licitacoes_all.xlsx", index=False)
 
    produtos_df = []
    for path in produtos:
        caminho = f"./files/{path}"
        try:
            licitacao = pd.read_excel(caminho)
        except Exception as e:
            print("erro no: ", caminho)
        produtos_df.append(licitacao)
    for path in produtos:
        if 'all' not in path:
            os.remove(f"./files/{path}")
    produtos_concat = pd.concat(produtos_df, ignore_index=True)
    produtos_concat.to_excel(f"./files/produtos_all.xlsx", index=False)
 
 
 
# Configurações driver
chrome_options = Options()
chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Desabilita imagens
prefs = {"profile.managed_default_content_settings.images": 2,
         "profile.default_content_setting_values.notifications": 2,
         "profile.managed_default_content_settings.stylesheets": 2,
         "profile.managed_default_content_settings.javascript": 1}  # Mantém JavaScript se necessário
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")  # Ativa o modo headless


# edge_options = Options()
# edge_options.use_chromium = True
# edge_options.add_argument("--headless")
# edge_options.add_argument("--blink-settings=imagesEnabled=false")

# prefs = {
#     "profile.managed_default_content_settings.images": 2,
#     "profile.default_content_setting_values.notifications": 2,
#     "profile.managed_default_content_settings.stylesheets": 2,
#     "profile.managed_default_content_settings.javascript": 1
# }
# edge_options.add_experimental_option("prefs", prefs)

# # Automatiza a instalação do driver e garante que esteja sempre atualizado.
# service = Service(EdgeChromiumDriverManager().install())
# prefs = {
#     "profile.managed_default_content_settings.images": 2,
#     "profile.default_content_setting_values.notifications": 2,
#     "profile.managed_default_content_settings.stylesheets": 2,
#     "profile.managed_default_content_settings.javascript": 1
# }
# edge_options.add_experimental_option("prefs", prefs)

# # Automatiza a instalação do driver e garante que esteja sempre atualizado.
# service = Service(EdgeChromiumDriverManager().install())
 
 
# Link do portal de compras
link="https://pncp.gov.br/app/editais?pagina=1&q=&status=recebendo_proposta"
 
# Listas para armazena as informações
local_list:list = []
orgao_list:list = []
unidade_list:list = []
modalidade_list:list = []
amparo_list:list = []
tipo_list:list = []
disputa_list:list = []
registro_list:list = []
data_divulgacao_PNCP_list:list = []
situação_list:list = []
inicio_list:list = []
fim_list:list = []
id_PNCP_list:list = []
fonte_list:list = []
objeto_list:list = []
termo_pesquisado:list = []
title_list:list = []
docs:list = []
item_desc_list:list = []
item_quantity_list:list = []
item_value_tot_list:list =[]
item_value_list:list = []
item_licitacao_id_list:list = []
value_is_present:list = []
 
def limpa_variaveis():
    global local_list, orgao_list, unidade_list, modalidade_list, amparo_list, tipo_list, disputa_list, registro_list
    global data_divulgacao_PNCP_list, situação_list, inicio_list, fim_list, id_PNCP_list, fonte_list
    global objeto_list, termo_pesquisado, title_list, docs, item_desc_list, item_quantity_list
    global item_value_tot_list, item_value_list, item_licitacao_id_list, value_is_present

    local_list = []
    orgao_list = []
    unidade_list = []
    modalidade_list = []
    amparo_list = []
    tipo_list = []
    disputa_list = []
    registro_list = []
    data_divulgacao_PNCP_list = []
    situação_list = []
    inicio_list = []
    fim_list = []
    id_PNCP_list = []
    fonte_list = []
    objeto_list = []
    termo_pesquisado = []
    title_list = []
    docs = []
    item_desc_list = []
    item_quantity_list = []
    item_value_tot_list =[]
    item_value_list = []
    item_licitacao_id_list = []
    value_is_present = []
# Funções Secundarias
def catch_append(driver,xpath, list_to_append):
    try:
        x = Wait(driver,xpath)
        list_to_append.append(x)
        # print(x)
    except Exception as e:
        # print(e)
        list_to_append.append(np.nan)
       
def Wait(driver,xpath):
    x = WebDriverWait(driver,5).until(
        EC.presence_of_element_located((By.XPATH,xpath))
    ).text
    return x
 
def isfrom_today(hoje, data):
    if data == hoje:
        return True
    else:
        return False
   
def isfrom_yesterday(data):
    hoje = datetime.now()
    ontem = hoje - timedelta(days=1)
    dia = ontem.day
    mes = ontem.month
    ano = ontem.year
    if mes < 10:
        mes = f"0{mes}"
    if dia < 10:
        dia = f"0{dia}"
    yesterday = f"{dia}/{mes}/{ano}"
    # print(today,data)
    if data == yesterday:
        return True
    else:
        return False
 
def is_equal(id_PNCP_list, href):
    def process_id(id_PNCP):
        id_PNCP = id_PNCP.split("-")
        del id_PNCP[1]
        id_PNCP[1] = id_PNCP[1].split("/")
        temp = id_PNCP[1][0].replace("0", "")
        id_PNCP[1][0] = id_PNCP[1][1]
        id_PNCP[1][1] = temp
        return '/'.join([str(item) if isinstance(item, str) else '/'.join(item) for item in id_PNCP])
 
    processed_ids = [process_id(id_PNCP) for id_PNCP in id_PNCP_list]
 
    href = ''.join(i for i in href if not i.isalpha())
    split_value = 0
    for i, c in enumerate(href):
        if c.isdigit():
            break
        split_value += 1
 
    href = href[split_value:]
 
    return any(pid == href for pid in processed_ids)
 
def click_and_catch_edital(driver):
    docs_temp:list=[]
    try:
        button_arquivos = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/pncp-tab-set/div/nav/ul/li[2]/button'))
        )
        driver.execute_script("arguments[0].focus();", button_arquivos)
        driver.execute_script("arguments[0].click();", button_arquivos)
        c = 1
        while True:
            try:
                father_xpath = f'//*[@id="main-content"]/pncp-item-detail/div/pncp-tab-set/div/pncp-tab[2]/div/div/pncp-table/div/ngx-datatable/div/datatable-body/datatable-selection/datatable-scroller/datatable-row-wrapper[{c}]/datatable-body-row/div[2]/'
                type_ = driver.find_element(By.XPATH,f'{father_xpath}datatable-body-cell[3]/div/span')
                if type_.text == "Edital":
                    document = driver.find_element(By.XPATH,f'{father_xpath}datatable-body-cell[4]/div/div/a')
                    href_document =document.get_attribute("href")
                    docs_temp.append(href_document)
                c+=1
                continue
            except:
                break
        string_docs = ""
        for i,n in enumerate(docs_temp):
            tot = len(docs_temp)
            string_docs += f"{n};" if i < tot else str(n)
        docs.append(string_docs)
        # print(docs)
       
    # button_arquivos.click()
    except Exception as e:
        print("Não Clicou no botão")
        print(e)
    return None
   
#Funções principais
def licitarDigital(link,value,dia):
 
    count_break = 0
    # Recebe o driver
    try: 
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e: 
        print(e)
        print("Erro no Driver")
        return None
    # driver = webdriver.Edge(service=service, options=edge_options)
    driver.implicitly_wait(5)
 
    # Configurações para os links
    q = value.replace(" ","%20")
 
    #Recebe o link ( a navegação é feita pela URL, os botões de navegação do site são bugados)
    try:
        driver.get(f"https://pncp.gov.br/app/editais?pagina=1&q={q}&status=recebendo_proposta")
    except Exception as e:
        print("Saiu sem encontrar licitação", e)
        return driver.quit()
    #Pega as paginas primarias enquanto estiver disponível
    while True:
        print("página principal")
        # Verifica se há o elemento "Essa busca não foi enontrada, se houver, quebra o looping"
        try:
            not_exists = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.XPATH,'//*[@id="main-content"]/pncp-list/pncp-results-panel/pncp-tab-set/div/pncp-show-messages/pncp-item-not-found/h2'))
                )
            return driver.quit()
        except:
            pass
 
        #Pega as licitações dentro da Pagina Principal
        count2=1
        while True:
            print("Segundo looping (Dentro das licitações)")
            try:
                path = f'//*[@id="main-content"]/pncp-list/pncp-results-panel/pncp-tab-set/div/pncp-tab[1]/div/div[2]/div/div[2]/pncp-items-list/div/div[{count2}]/a'
                licitação = WebDriverWait(driver,90).until(
                    EC.presence_of_element_located((By.XPATH,path))
                )                                        
               
                link2 = licitação.get_attribute('href')
                print(path)
                count2 +=1
               
                if is_equal(id_PNCP_list,link2):
                    # print("essa licitação já foi pega",value, (count2-1))
                    button_next_page = WebDriverWait(driver,30).until(
                        EC.presence_of_element_located((By.XPATH,'//button[@aria-label="Página seguinte"]'))
                    )
                    test = button_next_page.get_attribute("disabled")
                    quanty = WebDriverWait(driver,30).until(
                        EC.presence_of_element_located((By.XPATH,'//*[@id="main-content"]/pncp-list/pncp-results-panel/pncp-tab-set/div/pncp-tab[1]/div/div[1]/div/div[2]/span'))
                    ).text
                    quanty = quanty.split(" ")
                    quanty = quanty[0]
                    print(f"test: {test}, count2: {count2}, quanty: {quanty}")
                   
                    if (test is not None) and (int(count2) >= int(quanty)):
                        print("Caiu no quit")
                        return driver.quit()
                    else:
                        if int(count2) >= int(quanty):
                            print("debug")
                            pass
                        else:
                            print("recomeçando o looping")
                            pass
            except Exception as e:
                # print("Acabaram as licitações da pagina")
                print(e)
                try:
                    button_next_page = WebDriverWait(driver,30).until(
                        EC.presence_of_element_located((By.XPATH,'//button[@aria-label="Página seguinte"]'))
                    )
                except:
                    print("Error in endend licitations of the page")
                    return driver.quit()
               
                if button_next_page.get_attribute("disabled") is None:
                    driver.execute_script("arguments[0].click();",button_next_page)
                    print("O botão está ativo")
                    count2 = 1
                    continue
                else:
                    print("O botão está inativo")
                    return driver.quit()
            driver.get(link2)
 
            #Boa sorte
           
            for i in range(6):
                if i == 5:
                    # print("Não conseguiu, em nenhuma das tentativas, voltando")
                    driver.back()
                # print("loopando",(i+1),"/5")
               
                try:
                    local = WebDriverWait(driver,60).until(
                                EC.visibility_of_element_located((By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[1]/p/strong'))
                            )
                    # print("conseguiu")
                    break
                except Exception as e:
                    print("não conseguiu... reiniciando navegador")
                    return driver.quit()
                    #driver.refresh()
            try:
                local_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[1]/p/span')
            except:
                local_co = np.nan
            try:
                title = driver.find_element(By.XPATH,'//h1[@class="ng-star-inserted"]').text
            except:
                title = np.nan
            try:
                id_PNCP = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[6]/div[1]/p/strong')
                id_PNCP_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[6]/div[1]/p/span')
            except:
                id_PNCP = np.nan
                id_PNCP_co = np.nan
            try:
                orgao = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[2]/p/strong')
                orgao_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[2]/p/span')
            except:
                orgao = np.nan
                orgao_co = np.nan  
            try:
                unidade = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[3]/p/strong')
                unidade_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[3]/div[3]/p/span')
            except:
                unidade = np.nan
                unidade_co = np.nan    
            try:
                modalidade = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[1]/p/strong')
                modalidade_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[1]/p/span')
            except:
                modalidade = np.nan
                modalidade_co = np.nan      
            try:
                amparo = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[2]/p/strong')
                amparo_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[2]/p/span')
            except:
                amparo = np.nan
                amparo_co = np.nan    
            try:
                tipo = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[3]/p/strong')
                tipo_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[3]/p/span')
            except:
                tipo = np.nan
                tipo_co = np.nan
            try:
                disputa = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[4]/p/strong')
                disputa_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[4]/p/span')
            except:
                disputa = np.nan
                disputa_co = np.nan
            try:
                registro = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[5]/p/strong')
                registro_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[4]/div[5]/p/span')
            except:
                registro = np.nan
                registro_co = np.nan          
            try:
                data_divulgacao_PNCP = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[1]/p/strong')
                data_divulgacao_PNCP_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[1]/p/span')
            except:
                data_divulgacao_PNCP = np.nan
                data_divulgacao_PNCP_co = np.nan            
            try:
                situação = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[2]/p/strong')
                situação_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[2]/p/span')
            except:
                situação = np.nan
                situação_co = np.nan          
            try:
                inicio_recebimento_proposta = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[3]/p/strong')
                inicio_recebimento_proposta_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[3]/p/span')
            except:
                inicio_recebimento_proposta = np.nan
                inicio_recebimento_proposta_co = np.nan          
            try:
                fim_recebimento_proposta = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[4]/p/strong')
                fim_recebimento_proposta_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[5]/div[4]/p/span')
            except:
                fim_recebimento_proposta = np.nan
                fim_recebimento_proposta_co = np.nan          
            try:
                fonte = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[6]/div[2]/p/strong')
                fonte_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[6]/div[2]/p/span')
            except:
                fonte = np.nan
                fonte_co = np.nan          
            try:
                objeto = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[7]/p/strong')
                objeto_co = driver.find_element(By.XPATH,'//*[@id="main-content"]/pncp-item-detail/div/div[7]')
            except:
                objeto = np.nan
                objeto_co = np.nan        
            try:
                hoje = dia
                teste_data = isfrom_today(hoje, data_divulgacao_PNCP_co.text)
                print(hoje,data_divulgacao_PNCP_co.text)
            except:
                teste_data = False
 
            # teste_data = True # Gambiarra
            if teste_data == False:
                driver.back()
                count2 += 1
                count_break += 1
                # print("Não é de hoje")
                if count_break>= 5:
                    # print("count_break: ",count_break)
                    return driver.quit()
                else:
                    continue
            else:
                # print("é de hoje")
                try:
                    title_list.append(title)
                    local_list.append(f'{local_co.text}')
                    id_PNCP_list.append(f'{id_PNCP_co.text}')
                    orgao_list.append(f'{orgao_co.text}')
                    unidade_list.append(f'{unidade_co.text}')
                    modalidade_list.append(f'{modalidade_co.text}')
                    amparo_list.append(f'{amparo_co.text}')
                    tipo_list.append(f'{tipo_co.text}')
                    disputa_list.append(f'{disputa_co.text}')
                    registro_list.append(f'{registro_co.text}')
                    data_divulgacao_PNCP_list.append(f'{data_divulgacao_PNCP_co.text}')
                    situação_list.append(f'{situação_co.text}')
                    inicio_list.append(f'{inicio_recebimento_proposta_co.text}')
                    fim_list.append(f'{fim_recebimento_proposta_co.text}')
                    fonte_list.append(f'{fonte_co.text}')
                    objeto_list.append(f'{objeto_co.text}')

                except openpyxl.utils.exceptions.IllegalCharacterError as e:
                    title_list.append(np.nan)
                    local_list.append(np.nan)
                    id_PNCP_list.append(np.nan)
                    orgao_list.append(np.nan)
                    unidade_list.append(np.nan)
                    modalidade_list.append(np.nan)
                    amparo_list.append(np.nan)
                    tipo_list.append(np.nan)
                    disputa_list.append(np.nan)
                    registro_list.append(np.nan)
                    data_divulgacao_PNCP_list.append(np.nan)
                    situação_list.append(np.nan)
                    inicio_list.append(np.nan)
                    fim_list.append(np.nan)
                    fonte_list.append(np.nan)
                    objeto_list.append(np.nan)
                except: 
                    return driver.quit()  
                # Localize o campo de input
                time.sleep(2)
                try:
                    input_element = driver.find_element(By.XPATH, '//input[@type="text" and @aria-autocomplete="list"]')
                    achou = True
                except:
                    achou = False
 
                if achou:
                    driver.execute_script("arguments[0].focus();", input_element)
                    time.sleep(2)
                    try:
                        input_element.click()
                        input_element.send_keys(Keys.ARROW_UP)
                        input_element.send_keys(Keys.ARROW_UP)
                        input_element.send_keys(Keys.ENTER)
                    except:
                        print("Não foi possivel clicar, pegando de 5 em 5")
                else:
                    pass
 
                # Itera sobre a lista e pega os itens
                looping = True
                while looping:
                    # print("Entrou no looping da Tabela")
                    count3 = 1
                    while True:
                        # print("Está no Looping dos Produtos")
                        countP=1
                        have_produts = True
                        while have_produts:
                            try:
                                xpath_str = f'//*[@id="main-content"]/pncp-item-detail/div/pncp-tab-set/div/pncp-tab[1]/div/div/pncp-table/div/ngx-datatable/div/datatable-body/datatable-selection/datatable-scroller/datatable-row-wrapper[{countP}]/datatable-body-row/div[2]'
                                row_path = WebDriverWait(driver,10).until(
                                         EC.presence_of_element_located((By.XPATH,xpath_str))  
                                )
                            except:
                                have_produts = False
                            if have_produts:
                                catch_append(driver,f'{xpath_str}/datatable-body-cell[2]/div/span',item_desc_list)
                                catch_append(driver,f'{xpath_str}/datatable-body-cell[3]/div/span',item_quantity_list)
                                catch_append(driver,f'{xpath_str}/datatable-body-cell[4]/div/span',item_value_list)
                                catch_append(driver,f'{xpath_str}/datatable-body-cell[5]/div/span',item_value_tot_list)
                                termo_pesquisado.append(value)
                                item_licitacao_id_list.append(id_PNCP_co.text)
                                countP += 1
                        try:
                            button_next_list = WebDriverWait(driver,10).until(
                                EC.presence_of_element_located((By.XPATH,'//*[@id="btn-next-page"]'))  
                            )
                        except:
                            return driver.quit()
                        if button_next_list.get_attribute("disabled") is None:
                            driver.execute_script("arguments[0].click();", button_next_list)
                            continue
                        else:
                            click_and_catch_edital(driver)
                            driver.back()
                            looping = False
                            break
                   
                    time.sleep(0.2)
       
def run_workers(dia):
    termos_de_pesquisa = [
    "Jacto","Pulverizador","Pulverizador costal","Pulverizador costal a bateria","Bateria intercambiável","Pulverizador bateria recarregável","Pulverizador bateria de lítio","Novidade agro","Pulverizador elétrico",
    "Pulverizador costal elétrico","Pulverizador costal com bateria intercambiável","Guarany","Stihl","Matabi","Nagano","Pulverizador costal manual",
    "Pulverizador leve","Pulverizador costal mais leve","Pulverizador resistente","Pulverizador ergonômico","Tesoura de poda","Tesoura elétrica","Tesoura a bateria",
    "Ferramenta de poda","Podador a bateria","Podador elétrico","Podador profissional","Tesoura de poda profissional","Produtos agrícolas","Tecnologia agrícola","Inovação agro",
    "Produto inovador agrícola","Aplicador de grânulos","Aplicador de adubo","Aplicador de grânulos a bateria","Aplicador de insumos a bateria","Aplicador de adubo a bateria","Aplicador de grânulos elétrico",
    "Aplicador de adubo elétrico","Aplicador ergonômico","Aplicador com controle","Adubador elétrico","Aplicador de granulados costal","Aplicador de granulados a bateria","Adubadeira elétrica",
    "Adubadeira costal","Tesourão para poda","Tesourão profissional para poda","Acessórios para poda","Melhor acessório para poda","Tesourão robusto",
    "Tesourão potente","Tesourão resistente","Felco","Tramontina","Gardena","Truper","Dosador de grânulos","Aplicador e dosador de grânulos manuais","Dosador de insumos",
    "Dosador de adubo","Costais Jacto","Costal ergonômico","Espalhador de fertilizante","Espalhador de adubo","Distribuidor costal de grânulos","Distribuidor costal de adubo","Aplicador de fertilizantes",
    "Distribuidor de fertilizantes","Dosador de fertilizantes","Vonder","Adubadeira manual","EPI","Equipamento de proteção individual","Segurança do trabalho","Certificação QUEPIA","ISO 27065","Acessório para pulverizador costal","Acessório de pulverização costal","Barra 500","Barra para pulverização costal","Barra para pulverizar canteiro","Acessório para pulverizar canteiro","Aumentar a cobertura da pulverização"
    ]
    #termos_de_pesquisa = ["Pulverizador"]
    task_queue = queue.Queue()    
   
    for termo in termos_de_pesquisa:
        task_queue.put(termo)
   
    def worker():
        while not task_queue.empty():
            termo = task_queue.get()
            if termo is None:
                break
            licitarDigital(link, termo, dia)
            task_queue.task_done()
           
            print("TERMINOU A TAREFA", termo)
   
    # Número de threads
    num_threads = 6
       
    # Criando e iniciando threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=worker)
       
        thread.start()
       
        threads.append(thread)
   
    task_queue.join()
   
   
    for i in range(num_threads):
        task_queue.put(None)  
       
    for thread in threads:
        thread.join()
 
def make_excel():
    Licitação = {
    "local": local_list,
    "orgao": orgao_list,
    "unidade": unidade_list,
    "modalidade": modalidade_list,
    "amparo": amparo_list,
    "tipo": tipo_list,
    "disputa": disputa_list,
    "registro": registro_list,
    "data_divulgacao_PNCP": data_divulgacao_PNCP_list,
    "situação": situação_list,
    "inicio": inicio_list,
    "fim": fim_list,
    "id_PNCP": id_PNCP_list,
    "fonte": fonte_list,
    "objeto": objeto_list,
    "Documentos":docs,
    "Titulo Edital":title_list
    }
    Produtos_Licitação = {
        "id_PNCP": item_licitacao_id_list,
        "Produto":item_desc_list,
        "Valor":item_value_list,
        "Valor Total": item_value_tot_list,
        "Quantidade":item_quantity_list,
        "Termo Pesquisado": termo_pesquisado
   
    }
    df = pd.DataFrame(Licitação)
    df.drop_duplicates(subset='id_PNCP',inplace=True)
   
    df_2 = pd.DataFrame(Produtos_Licitação)
    df_2.drop_duplicates(subset=['id_PNCP','Produto'],inplace=True)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_excel(f'./files/licitacoes-{today}.xlsx')
    df_2.to_excel(f'./files/produtos-{today}.xlsx')  
    # df.to_excel(f'./files/licitacoes_base.xlsx')
    # df_2.to_excel(f'./files/produtos_base.xlsx')  
 
 
def send_mail():
    termos_de_pesquisa = [
    "Pulverizador",
    "Pulverizador costal a bateria",
    "Pulverizador elétrico costal",
    "Pulverizador com bateria intercambiável",
    "Acessório para pulverização costal",
    "Tesoura de poda",
    "Tesoura de poda elétrica",
    "Podador a bateria",
    "Podador elétrico",
    "Tesourão de poda profissional a bateria",
    "Tesourão de poda profissional elétrico",
    "Acessórios para poda",
    "Aplicador de grânulos a bateria",
    "Aplicador de grânulos elétrico",
    "Aplicador de adubo a bateria",
    "Aplicador de adubo elétrico",
    "Adubadeira elétrica costal",
    "Dosador de grânulos",
    "Dosador de adubo",
    "Espalhador de fertilizantes costal",
    "Espalhador de adubo costal",
    "Novidade agro",
    "Tecnologia agrícola",
    "Inovação agro",
    "Produto inovador agrícola",
    "Jacto",
    "Serrote Poda",
    "Poda",
    "Guarany",
    "Stihl",
    "Felco"]
 
    termos_exclusão = ['34ml','34 ml','500 ml', '500ml', '1Litro', '1 litro', '1L', '1 L','Borrifador','alcool','afiador','liga','curva',
                       '1 litro.','Litro.','Litro','500','multiuso','cirurgica','cirúrgica','cirúrgicas','cirurgicas','iris','irís','instrumental','romba',
                       'inox','costura','escolar','ires','íres','reta','papel','inoxidável','inoxidavel','unhas','unha','automotivo','multiuso','multi','bisturi',
                       'serviço','servico']
    termos_exclusão = [term.lower() for term in termos_exclusão]
 
    today = datetime.now().strftime("%Y-%m-%d")
    # termos_de_pesquisa = ["Tesoura de Poda", "Pulverizador"]
    termos_de_pesquisa_normalizado = [term.lower() for term in termos_de_pesquisa]
    data = pd.read_excel(f"./files/licitacoes-{today}.xlsx")
   
    produtos = pd.read_excel(f"./files/produtos-{today}.xlsx")
    produtos['encontrou_termo'] = np.nan 

    # produtos['encontrou_termo'] = produtos.apply(
    #     lambda x: any(term in termos_de_pesquisa for term in str(x['Produto']).split(" ")) if isinstance(x['Produto'], str) else False,
    #     axis=1
    # )
    # print(produtos['Produto'])
 
    r_list = []
    for i, r in enumerate(produtos['Produto']):
        achou = False
   
       
        if isinstance(r,str):
            r_list = r.split(" ")
        # print(r_list)
        if r_list == []:
            print("não tem nada")
        if r_list == np.nan:
            print("é nan")
       
        for j in range(len(r_list)):
 
            # print(r_list[j])
            # print(r_list[j].lower())
            if r_list[j].lower() in termos_de_pesquisa_normalizado:
                achou = True
                # print("Achou")
                # print(r_list[j])
                another_r_list = r.lower().split(" ")
                for id, item in enumerate(another_r_list):
                    if item.replace(",","").replace(".","") in termos_exclusão:
                        achou = False
                       
        produtos["encontrou_termo"][i] = False
       
        if achou == True:
            produtos["encontrou_termo"][i] = True
               
       
    # print(produtos[produtos['encontrou_termo'] == True])
 
    # produtos
    # produtos.to_excel('produtos.xlsx')
       
    merge = pd.merge(data,produtos, on='id_PNCP',how='inner')
    merge.drop_duplicates(subset=['id_PNCP','Produto','Valor','Quantidade','Valor Total'],inplace=True)
    result = merge['encontrou_termo'] == True
    
   
   
   
   
    licitações = merge.groupby('id_PNCP')
    mensagens = []
   
    for id_pncp, grupo in licitações:
 
        produtos_f = grupo['Produto']
       
        # Adicionar Coluna de Edital
        coluna_id = id_pncp
        coluna_title = grupo['Titulo Edital'].iloc[0]
        coluna_local = grupo['local'].iloc[0]
        coluna_orgao = grupo['orgao'].iloc[0]
        docs = grupo['Documentos'].iloc[0].replace(";", "") if isinstance(grupo['Documentos'].iloc[0], str) else ""
   
        produtos_desc = grupo.loc[grupo['encontrou_termo'] == True, 'Produto'].reset_index(drop=True).to_list()
        produtos_quantidade = grupo.loc[grupo['encontrou_termo'] == True, 'Quantidade'].reset_index(drop=True).to_list()
        produtos_valor = grupo.loc[grupo['encontrou_termo'] == True, 'Valor'].reset_index(drop=True).to_list()
        produtos_valor_total = grupo.loc[grupo['encontrou_termo'] == True, 'Valor Total'].reset_index(drop=True).to_list()
   
        part_msg = ""
       
        if len(produtos_desc) >= 1:
            for i in range(len(produtos_desc)):
                part_msg += f"""
                    <tr>
                        <td style='padding: 8px; text-align: left;'>{produtos_desc[i]}</td>
                        <td style='padding: 8px; text-align: left;'>{produtos_quantidade[i]}</td>
                        <td style='padding: 8px; text-align: left;'>{produtos_valor[i]}</td>
                        <td style='padding: 8px; text-align: left;'>{produtos_valor_total[i]}</td>
                    </tr>
                    """
                if docs == "":
                    docs_msg = "<p>Não havia Edital disponível</p>"
                else:
                    docs_msg =f"""<a style='display: block; width: 99%; text-decoration: none; background-color: #0A5193; color: #FFFFFF; padding: 10px; border-radius: 8px; text-align: center; margin-top: 10px;' href="{docs}" class="button">Baixar Edital</a>"""
                mensagem = f"""
                <div style='margin-bottom: 20px; border: 1px solid #0A5193; border-radius: 8px; padding: 15px; background-color: #f8f9fa;'>
                    <ul style='list-style:none;padding:0;margin:0;'>
                        <li><strong style='color: #0A5193'>Id Licitação PNCP:</strong> {coluna_id}</li>
                        <li><strong style='color: #0A5193'>Edital:</strong> {coluna_title}</li>
                        <li><strong style='color: #0A5193'>Local:</strong> {coluna_local}</li>
                        <li><strong style='color: #0A5193'>Orgão:</strong> {coluna_orgao}</li>
                        <li><strong style='color: #0A5193'>Produtos:</strong></li>
                    </ul>
                    <table style="width: 100%; margin-top: 10px; border-collapse: collapse;">
                        <thead>
                            <tr>
                                <th style='background-color: #0a5193; color: #FFFFFF; padding: 8px; text-align: left;'>Descrição</th>
                                <th style='background-color: #0a5193; color: #FFFFFF; padding: 8px; text-align: left;'>Quantidade</th>
                                <th style='background-color: #0a5193; color: #FFFFFF; padding: 8px; text-align: left;'>Valor</th>
                                <th style='background-color: #0a5193; color: #FFFFFF; padding: 8px; text-align: left;'>Valor Total</th>
                            </tr>
                        </thead>
                        <tbody>
                            {part_msg}
                        </tbody>
                    </table>
                    {docs_msg}
                </div>
                """
            mensagens.append(mensagem)
   
    email = f"""
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; width: 100%; background-color: #f2f2f2;">
        <h1 style='color: #0A5193; text-align: center;'>Novas Licitações</h1>
    """
   
    for i, mensagem in enumerate(mensagens):
        email += mensagem
        if i < len(mensagens) - 1:
            email += "<hr style='border: 1px solid #0A5193;'>"
   
    email += """
    </body>
    """
    webhook_link = os.getenv("WEBHOOK_LINK")
    webhook_url = webhook_link
    if mensagens:
        payload = {
            "email":email.replace('\n','')
        }
    else:
        payload = {
            "email":"Não apareceram novas licitações"
        }        
       
    headers = {
            "Content-Type": "application/json"
        }
   
    response = requests.post(webhook_url, json=payload, headers=headers)    
 
    if response.status_code == 200 or response.status_code == 202:
            print("E-mail enviado com sucesso")
    else:
        print(f"Failed to send webhook. Status code: {response.status_code}")
    
# Função Main
def execute_scripts(dia):
    run_workers(dia)
    make_excel()
    send_mail()
    limpa_variaveis()



    
if __name__ == "__main__":
    print("ITS WORKS!!!")
