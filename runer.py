from Scraper_Licitações import execute_scripts, limpa_arquivos
from datetime import datetime
import time
 
# Format the date and time as a string
marker = "08:00"
 
for_test = True
 
 
while True:
    dia = datetime.now().strftime("%d/%m/%Y")
    now = datetime.now().strftime("%H:%M")

    if marker == now:
        execute_scripts(dia)
        dia_l = datetime.now().strftime("%d")
        if dia_l == "01":
            limpa_arquivos()
    else:
        time.sleep(60)
