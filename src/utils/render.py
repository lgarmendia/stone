"""Render."""

import os
import sys
import yaml


def get_yaml_value(param: str) -> any:
    """Obter o parâmetro do arquivo YAML.

    :param param: parâmetro a ser renderizado.
    :return: o valor de um determinado parâmetro do arquivo YAML.
    """
    # yaml_path = "/opt/airflow/configs/params.yaml"
    yaml_path = "/opt/airflow/configs/params.yaml"
    yaml_path = os.path.join(sys.path[1], yaml_path)
    try:
        with open(f"{yaml_path}", "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        raise e
    return config[param]
