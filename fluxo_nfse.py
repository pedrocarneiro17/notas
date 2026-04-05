from playwright.sync_api import sync_playwright
import re
import time
import os
import sys
import glob

os.environ["NODE_OPTIONS"] = "--openssl-legacy-provider"

_contexto_ativo    = None
_pagina_ativa      = None
_emissao_cancelada = False

def cancelar_emissao():
    global _contexto_ativo, _pagina_ativa, _emissao_cancelada
    _emissao_cancelada = True
    if _pagina_ativa:
        try:
            _pagina_ativa.close()
        except Exception:
            pass
        _pagina_ativa = None
    _contexto_ativo = None


def _pasta_base() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _achar_chromium() -> str:
    """Localiza o executável do Chromium em qualquer lugar do PC."""
    candidatos = []

    # 1) Bundled pelo PyInstaller (_internal/...)
    base = _pasta_base()
    candidatos += glob.glob(os.path.join(
        base, "_internal", "playwright", "driver", "package",
        ".local-browsers", "chromium-*", "chrome-win*", "chrome.exe"
    ))

    # 2) ms-playwright padrão do Windows
    local_app = os.environ.get("LOCALAPPDATA", "")
    user_profile = os.environ.get("USERPROFILE", "")
    for raiz in [local_app, user_profile]:
        if raiz:
            candidatos += glob.glob(os.path.join(
                raiz, "ms-playwright", "chromium-*", "chrome-win*", "chrome.exe"
            ))

    # 3) Variável de ambiente PLAYWRIGHT_BROWSERS_PATH
    pw_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "")
    if pw_path:
        candidatos += glob.glob(os.path.join(
            pw_path, "chromium-*", "chrome-win*", "chrome.exe"
        ))

    # Retorna o primeiro encontrado (mais recente por nome, ordem decrescente)
    candidatos = [c for c in candidatos if os.path.isfile(c)]
    if candidatos:
        return sorted(candidatos)[-1]
    return ""  # Playwright usará o padrão interno


def _pasta_downloads() -> str:
    import config as _cfg
    pasta = _cfg.get("pasta_downloads") or os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(pasta, exist_ok=True)
    return pasta


def _valor_para_float(valor_fmt: str) -> float:
    """Converte '2.000,00' (BR) para float 2000.0"""
    return float(valor_fmt.replace(".", "").replace(",", "."))


def _float_para_br(v: float) -> str:
    """Converte float para formato BR '2.000,00'"""
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _formatar_valor(valor: str) -> str:
    """
    Garante que o valor esteja no formato esperado pelo portal: 2.000,00
    Aceita entradas como: "2000,00" / "2.000,00" / "2000.00" / "2000"
    """
    valor = valor.strip().replace(" ", "")

    if "." in valor and "," not in valor:
        partes = valor.split(".")
        if len(partes[-1]) == 2:      # ponto é decimal (formato americano)
            inteiro = "".join(partes[:-1])
            decimal = partes[-1]
        else:                          # ponto é separador de milhar
            inteiro = valor.replace(".", "")
            decimal = "00"
    elif "," in valor:
        partes = valor.split(",")
        inteiro = partes[0].replace(".", "")
        decimal = partes[1] if len(partes) > 1 else "00"
    else:
        inteiro = valor
        decimal = "00"

    decimal = decimal.ljust(2, "0")[:2]
    inteiro_fmt = f"{int(inteiro):,}".replace(",", ".")
    return f"{inteiro_fmt},{decimal}"


def _is_cep_bh(cep: str) -> bool:
    """Retorna True se o CEP pertence à faixa de Belo Horizonte (30000-000 a 31999-999)."""
    numeros = re.sub(r"\D", "", cep)
    if len(numeros) != 8:
        return False
    return 30000000 <= int(numeros) <= 31999999


def _codigo_complementar_bh(pagina, codigo_tributacao: str):
    """Preenche o campo CodigoComplementarMunicipal para clientes de BH."""
    cod = re.sub(r"\D", "", codigo_tributacao)[:6]

    pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen a").click()
    time.sleep(2)

    if cod == "171201":
        pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen").get_by_text("17.12.01.003 - Administração").click()
    elif cod == "040101":
        pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen").get_by_text("- Medicina").click()


def emitir_nfse(dados: dict):
    """
    Recebe um dicionário com os dados do emitente e da nota e executa o fluxo
    de emissão no Portal Nacional de NFS-e via Playwright.

    Chaves esperadas em `dados`:
        caminho_certificado (str)  - caminho absoluto do .pfx
        senha_certificado   (str)  - senha do certificado
        cep                 (str)  - CEP do emitente (ex: "35.402-179")
        lucro_presumido     (bool) - True = Lucro Presumido, False = Simples Nacional
        tipo_doc_tomador    (str)  - "CPF" ou "CNPJ"
        inscricao_tomador   (str)  - CPF ou CNPJ do tomador (ex: "113.972.066-08")
        data_competencia    (str)  - data no formato DD/MM/YYYY
        local_prestacao     (str)  - nome do município (ex: "Ouro Preto")
        descricao_servico   (str)  - texto livre de descrição
        valor_servico       (str)  - valor formatado (ex: "200,00")
        codigo_nbs          (str)  - código NBS (ex: "101011200")
        codigo_tributacao   (str)  - código de tributação (ex: "01.07.01")
        cep_tomador         (str)  - CEP do tomador (ex: "35.400-541")
        numero_tomador      (str)  - número do endereço do tomador
        complemento_tomador (str)  - complemento (pode ser vazio)
    """

    caminho_cert        = dados["caminho_certificado"]
    senha_cert          = dados["senha_certificado"]
    cep                 = dados["cep"]
    lucro_presumido     = dados["lucro_presumido"]
    tipo_doc_tomador    = dados["tipo_doc_tomador"]
    inscricao_tomador   = dados["inscricao_tomador"]
    sem_cep_tomador     = dados.get("sem_cep_tomador", False)
    cep_tomador         = dados.get("cep_tomador", "")
    numero_tomador      = dados.get("numero_tomador", "")
    complemento_tomador = dados.get("complemento_tomador", "")
    data                = dados["data_competencia"]
    local               = dados["local_prestacao"]
    descricao           = dados["descricao_servico"]
    valor               = dados["valor_servico"]
    codigo_nbs          = dados["codigo_nbs"]
    codigo_tributacao   = dados["codigo_tributacao"]
    retencao_issqn      = dados["retencao_issqn"]
    aliquota_issqn      = dados.get("aliquota_issqn", "")
    obra                = dados.get("obra", False)
    cep_obra            = dados.get("cep_obra", "")
    numero_obra         = dados.get("numero_obra", "")
    complemento_obra    = dados.get("complemento_obra", "")

    with sync_playwright() as p:
        perfil = os.path.join(_pasta_base(), "browser_profile")
        os.makedirs(perfil, exist_ok=True)

        global _contexto_ativo, _pagina_ativa, _emissao_cancelada
        _emissao_cancelada = False
        chrome_exe = _achar_chromium()
        launch_kwargs = dict(
            user_data_dir=perfil,
            headless=False,
            ignore_https_errors=True,
            accept_downloads=True,
            client_certificates=[{
                "origin": "https://www.nfse.gov.br",
                "pfxPath": caminho_cert,
                "passphrase": senha_cert
            }]
        )
        if chrome_exe:
            launch_kwargs["executable_path"] = chrome_exe
        contexto = p.chromium.launch_persistent_context(**launch_kwargs)
        _contexto_ativo = contexto
        pagina = contexto.new_page()
        _pagina_ativa = pagina

        # ── [1] Login ───────────────────────────────────────────────
        print("[1] Acessando página de login...")
        pagina.goto("https://www.nfse.gov.br/EmissorNacional/Login")

        print("[2] Login com certificado digital...")
        pagina.locator("a.img-certificado").click()
        pagina.wait_for_load_state("domcontentloaded")

        # ── [2] Abrindo nova NFS-e ──────────────────────────────────
        print("[3] Abrindo Nova NFS-e (Emissão Completa)...")
        pagina.get_by_role("button").filter(has_text="Nova NFS-e").click()
        pagina.get_by_role("link", name="Emissão completa").click()

        # ── [3] Data de Competência ─────────────────────────────────
        print(f"[4] Preenchendo data de competência: {data}")
        pagina.locator("#DataCompetencia").fill(data)
        pagina.locator("#DataCompetencia").press("Enter")
        pagina.locator("body").click()

        # ── [4] Regime tributário (apenas Simples Nacional) ──────────
        if not lucro_presumido:
            print("[5] Regime tributário: Simples Nacional")
            pagina.locator("#SimplesNacional_RegimeApuracaoTributosSN_chosen a").filter(
                has_text="Selecione..."
            ).click()
            pagina.locator("#SimplesNacional_RegimeApuracaoTributosSN_chosen").get_by_text(
                "Regime de apuração dos tributos federais e municipal pelo Simples Nacional"
            ).click()

        # ── [5] Dados do emitente (CEP) ──────────────────────────────
        print(f"[6] Preenchendo CEP: {cep}")
        pagina.get_by_role("button", name="Exibir detalhes do emitente").click()
        pagina.locator("#Prestador_EnderecoNacional_CEP").click()
        pagina.locator("#Prestador_EnderecoNacional_CEP").fill(cep)
        pagina.locator("body").click()

        # ── [5b] Dados do tomador ─────────────────────────────────
        print(f"[6b] Preenchendo {tipo_doc_tomador} do tomador: {inscricao_tomador}")
        pagina.locator("#pnlTomador").get_by_text("Brasil").click()
        pagina.locator("#Tomador_Inscricao").fill(inscricao_tomador)
        pagina.locator("#btn_Tomador_Inscricao_pesquisar").click()
        pagina.wait_for_load_state("networkidle")

        if not sem_cep_tomador:
            print(f"[6c] Preenchendo endereço do tomador: CEP {cep_tomador}, nº {numero_tomador}")
            digits = re.sub(r"\D", "", inscricao_tomador)
            if len(digits) <= 11:  # CPF
                pagina.locator("#pnlTomadorInformarEnderecoCheck label").click()
            campo_cep = pagina.locator("#Tomador_EnderecoNacional_CEP")
            campo_cep.click()
            pagina.locator("#Tomador_EnderecoNacional_CEP").fill("")
            campo_cep.fill(cep_tomador)
            pagina.locator("#btn_Tomador_EnderecoNacional_CEP").click()
            pagina.wait_for_load_state("networkidle")
            time.sleep(2)
            pagina.locator("#Tomador_EnderecoNacional_Numero").fill("")
            pagina.locator("#Tomador_EnderecoNacional_Numero").fill(numero_tomador)
            if complemento_tomador:
                pagina.locator("#Tomador_EnderecoNacional_Complemento").fill("")
                pagina.locator("#Tomador_EnderecoNacional_Complemento").fill(complemento_tomador)
        else:
            print("[6c] CEP do tomador ignorado (sem_cep_tomador=True)")

        pagina.get_by_role("button", name="Avançar").click()
        pagina.wait_for_load_state("domcontentloaded")

    
        # ── [6] Local de prestação ───────────────────────────────────
        print(f"[7] Local de prestação: {local}")
        pagina.locator("#pnlLocalPrestacao").get_by_label("").click()
        pagina.get_by_role("searchbox", name="Search").press("CapsLock")
        pagina.get_by_role("searchbox", name="Search").fill(local)
        time.sleep(1)
        if local.strip().lower() in ("ouro preto", "mariana"):
            pagina.get_by_role("searchbox", name="Search").press("ArrowDown")
        pagina.get_by_role("searchbox", name="Search").press("Enter")

        # ── [7] Código de tributação (serviço) ───────────────────────
        print(f"[8] Selecionando código de tributação {codigo_tributacao}...")
        pagina.locator("#pnlServicoPrestado").get_by_label("", exact=True).click()
        time.sleep(1)
        pagina.get_by_role("searchbox", name="Search").fill(codigo_tributacao)
        time.sleep(1)
        pagina.get_by_role("searchbox", name="Search").press("Enter")

        # ── [7b] Código complementar municipal (somente BH) ─────────
        if _is_cep_bh(cep):
            print(f"[8b] CEP de BH detectado — preenchendo código complementar municipal...")
            _codigo_complementar_bh(pagina, codigo_tributacao)

        # ── [8] Dados do serviço ─────────────────────────────────────
        print(f"[9] Descrição: {descricao}")
        pagina.locator("#pnlServicoPrestado").get_by_text("Não", exact=True).click()
        pagina.locator("#ServicoPrestado_Descricao").fill(descricao)

        # ── [9] Código NBS ───────────────────────────────────────────
        print(f"[10] Código NBS: {codigo_nbs}")

        pagina.locator("#ServicoPrestado_CodigoNBS_chosen").click()
        time.sleep(1)
        pagina.locator("#ServicoPrestado_CodigoNBS_chosen input").fill(codigo_nbs)
        time.sleep(1)
        pagina.locator("#ServicoPrestado_CodigoNBS_chosen input").press("ArrowDown")
        time.sleep(1)
        pagina.locator("#ServicoPrestado_CodigoNBS_chosen input").press("Enter")


        # ── [9b] Endereço da obra ────────────────────────────────────
        if obra:
            print(f"[10b] Preenchendo endereço da obra: CEP {cep_obra}, nº {numero_obra}")
            pagina.locator("#pnlObraInformada").get_by_text("Endereço no Brasil").click()
            pagina.locator("#Obra_CEP").fill(cep_obra)
            pagina.locator("body").click()
            pagina.wait_for_load_state("networkidle")
            pagina.locator("#Obra_Numero").fill(numero_obra)
            if complemento_obra:
                pagina.locator("#Obra_Complemento").fill(complemento_obra)

        pagina.get_by_role("button", name="Avançar").click()

        # ── [10] Valor do serviço ────────────────────────────────────
        # Normaliza: aceita tanto "2000,00" quanto "2.000,00" → garante "2.000,00"
        valor_normalizado = _formatar_valor(valor)
        print(f"[11] Valor do serviço: R$ {valor_normalizado}")
        pagina.locator("#Valores_ValorServico").fill(valor_normalizado)
        #pagina.locator("#Valores_ValorServico").press("Enter")
        pagina.locator("body").click()


        # ── [11] Retenção de ISSQN ───────────────────────────────────
        if lucro_presumido:
            pagina.get_by_text("Não").nth(1).click()

        if retencao_issqn:
            print(f"[12] Retenção ISSQN: Sim")
            if lucro_presumido:
                pagina.get_by_text("Sim").nth(2).click()
            else:
                pagina.get_by_text("Sim").nth(3).click()
                print("oi")
                
            pagina.get_by_text("Retido pelo Tomador").click()
            print("oi2")
            
            if not lucro_presumido and aliquota_issqn:
                print("oi3")
                time.sleep(5)
                pagina.locator("#ISSQN_AliquotaInformada").click()
                pagina.locator("#ISSQN_AliquotaInformada").fill(aliquota_issqn)
        
        if not retencao_issqn:
            print(f"[12] Retenção ISSQN: Não")
            pagina.get_by_text("Não").nth(3).click()        

        if lucro_presumido:
            pagina.get_by_text("Não").nth(2).click()
            pagina.get_by_text("Não").nth(5).click()

            
        elif obra:
            pagina.get_by_text("Não").nth(5).click()
            pagina.get_by_text("Não", exact=True).nth(3).click()

        # ── [12] Tributação Federal (PIS/COFINS) ─────────────────────
        print("[13] Selecionando tributação federal...")

        if lucro_presumido:
            pagina.locator("#TributacaoFederal_PISCofins_SituacaoTributaria_chosen a").filter(has_text="Selecione...").click()
            pagina.locator("#TributacaoFederal_PISCofins_SituacaoTributaria_chosen").get_by_text("01 - Operação Tributável com").click()
            time.sleep(1)
            pagina.locator("#TributacaoFederal_PISCofins_BaseDeCalculo").wait_for(state="visible", timeout=10000)
            pagina.locator("#TributacaoFederal_PISCofins_BaseDeCalculo").fill(valor_normalizado)
            pagina.locator("#TributacaoFederal_PISCofins_AliquotaPIS").fill("0,065")
            pagina.locator("#TributacaoFederal_PISCofins_AliquotaCOFINS").fill("0,300")
            pagina.locator("#TributacaoFederal_PISCofins_TipoRetencao_chosen a").filter(has_text="Selecione...").click()
            pagina.locator("#TributacaoFederal_PISCofins_TipoRetencao_chosen").get_by_text("PIS/COFINS/CSLL Retidos").click()
            
            time.sleep(2)
            valor_float = _valor_para_float(valor_normalizado)
            irrf = pagina.locator("#TributacaoFederal_ValorIRRF")
            irrf.wait_for(state="visible", timeout=10000)
            pagina.locator("#TributacaoFederal_ValorIRRF").fill(_float_para_br(valor_float * 0.015))
            pagina.locator("#TributacaoFederal_ValorCSLL").fill(_float_para_br(valor_float * (0.0065 + 0.03 + 0.01))
            )
        else:
            pagina.evaluate("""() => {
                const sel = document.getElementById('TributacaoFederal_PISCofins_SituacaoTributaria');
                const opt = Array.from(sel.options).find(o => o.text.includes('Nenhum'));
                if (opt) { sel.value = opt.value; $(sel).trigger('change').trigger('chosen:updated'); }
            }""")
            pagina.evaluate("""() => {
                const sel = document.getElementById('TributacaoFederal_PISCofins_TipoRetencao');
                const opt = Array.from(sel.options).find(o => o.text.includes('Não Retidos'));
                if (opt) { sel.value = opt.value; $(sel).trigger('change').trigger('chosen:updated'); }
            }""")

        # ── [13] Salvar e baixar arquivos ────────────────────────────
        print("[14] Avançando para emissão final...")
        pagina.get_by_role("button", name="Avançar").click()
        pagina.wait_for_load_state("domcontentloaded")
        
        '''
       pagina.locator("#btnProsseguir").click()
        pagina.wait_for_load_state("domcontentloaded")

        print("[15] Baixando XML...")
        with pagina.expect_download() as dl_xml:
            pagina.get_by_role("link", name="Baixar XML").click()
        nome_xml = dl_xml.value.suggested_filename or "nfse.xml"
        if not nome_xml.lower().endswith(".xml"):
            nome_xml += ".xml"
        dl_xml.value.save_as(os.path.join(_pasta_downloads(), nome_xml))
        print(f"[15] XML salvo: {nome_xml}")

        print("[16] Baixando DANFSe...")
        with pagina.expect_download() as dl_pdf:
            pagina.get_by_role("link", name="Baixar DANFSe").click()
        nome_pdf = dl_pdf.value.suggested_filename or "nfse.pdf"
        if not nome_pdf.lower().endswith(".pdf"):
            nome_pdf += ".pdf"
        dl_pdf.value.save_as(os.path.join(_pasta_downloads(), nome_pdf))
        print(f"[16] DANFSe salvo: {nome_pdf}")

        #print("✅ NFS-e emitida e arquivos salvos em /downloads")
        '''

        # Mantém o browser aberto até o usuário fechar a guia manualmente
        try:
            pagina.wait_for_event("close", timeout=0)
        except Exception:
            pass

        _pagina_ativa = None
        if _emissao_cancelada:
            raise Exception("browser has been closed")


        _contexto_ativo = None