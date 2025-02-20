import os
import zipfile
import logging
import shutil
from datetime import datetime
from src.utils import render

# Configuração de logging apenas no console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Variaveis
INPUT_FOLDER = render.get_yaml_value("input_folder")  
BRONZE_LOCATION = render.get_yaml_value("bronze_location")  
PROCESS_FOLDER = render.get_yaml_value("process_folder")  

def get_zip_files(directory):
    """ Retorna arquivos ZIP que contêm 'socio' ou 'empresas' no nome. """
    return [os.path.join(directory, f) for f in os.listdir(directory) 
            if f.lower().endswith(".zip") and ("socio" in f.lower() or "empresas" in f.lower())]

def extract_and_rename(zip_path, target_folder):
    """ 
    Extrai o arquivo ZIP, renomeando-o corretamente para CSV.
    Fecha o ZIP antes de movê-lo para evitar erro de arquivo em uso.
    """
    try:
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if not file_name.endswith("/"):  #
                    # Determina o tipo do arquivo baseado no nome do ZIP
                    category = "socio" if "socio" in zip_path.lower() else "empresas"
                    new_name = f"{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    target_path = os.path.join(target_folder, new_name)

                    # Extrai e renomeia o arquivo CSV
                    extracted_path = zip_ref.extract(file_name, target_folder)
                    os.rename(extracted_path, target_path)

                    logger.info(f"Extraído: {new_name}")
                    break

        shutil.move(zip_path, os.path.join(PROCESS_FOLDER, os.path.basename(zip_path)))
        logger.info(f"ZIP movido para {PROCESS_FOLDER}")

    except Exception as e:
        logger.error(f"Erro ao processar {zip_path}: {e}")


for zip_file in get_zip_files(INPUT_FOLDER):
    print('ESTA ENTRANDO AQUI')
    extract_and_rename(zip_file, BRONZE_LOCATION)
