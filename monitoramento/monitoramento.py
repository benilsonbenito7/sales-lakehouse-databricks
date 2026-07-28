from datetime import datetime

class CorTerminal:
    RESET = "\033[0m"
    AZUL = "\033[94m"
    AMARELO = "\033[93m"
    VERMELHO = "\033[91m"
    VERDE= "\033[32m"

def registrar_info(mensagem: str) -> None:
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {CorTerminal.AZUL}[INFO]{CorTerminal.RESET} {mensagem}"
    )

def registrar_warning(mensagem: str) -> None:
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {CorTerminal.AMARELO}[WARNING]{CorTerminal.RESET} {mensagem}"
    )

def registrar_error(mensagem: str) -> None:
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {CorTerminal.VERMELHO}[ERROR]{CorTerminal.RESET} {mensagem}"
    )