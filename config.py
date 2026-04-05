import json
import os
import sys


def _pasta_base() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _config_path() -> str:
    return os.path.join(_pasta_base(), "config.json")


def get(chave: str, padrao=None):
    try:
        with open(_config_path(), "r", encoding="utf-8") as f:
            return json.load(f).get(chave, padrao)
    except Exception:
        return padrao


def salvar(chave: str, valor):
    path = _config_path()
    dados = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            dados = json.load(f)
    except Exception:
        pass
    dados[chave] = valor
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
