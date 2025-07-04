import pandas as pd
import numpy as np
produtos = pd.read_excel('./files/produtos_all.xlsx')
data = pd.read_excel('./files/licitacoes_all.xlsx')


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

# termos_de_pesquisa = ["Tesoura de Poda", "Pulverizador"]
termos_de_pesquisa_normalizado = [term.lower() for term in termos_de_pesquisa]

produtos['encontrou_termo'] = np.nan


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

merge.to_excel("vieww.xlsx")