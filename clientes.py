import json
import os
import sys

def _pasta_exe() -> str:
    """Retorna a pasta do executável (PyInstaller) ou do script (desenvolvimento)."""
    if getattr(sys, "frozen", False):
        # Rodando como .exe gerado pelo PyInstaller
        return os.path.dirname(sys.executable)
    # Rodando como script Python normal
    return os.path.dirname(os.path.abspath(__file__))

ARQUIVO_CLIENTES = os.path.join(_pasta_exe(), "clientes.json")


def _carregar() -> dict:
    if not os.path.exists(ARQUIVO_CLIENTES):
        return {}
    with open(ARQUIVO_CLIENTES, "r", encoding="utf-8") as f:
        return json.load(f)


def _salvar(dados: dict):
    with open(ARQUIVO_CLIENTES, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def listar_clientes() -> list[str]:
    """Retorna lista de nomes cadastrados em ordem alfabética."""
    return sorted(_carregar().keys())


def carregar_cliente(nome: str) -> dict | None:
    """Retorna os dados de um cliente pelo nome, ou None se não existir."""
    return _carregar().get(nome)


def salvar_cliente(nome: str, dados: dict):
    """Cadastra ou atualiza um cliente."""
    clientes = _carregar()
    clientes[nome] = dados
    _salvar(clientes)


def deletar_cliente(nome: str):
    """Remove um cliente pelo nome."""
    clientes = _carregar()
    clientes.pop(nome, None)
    _salvar(clientes)