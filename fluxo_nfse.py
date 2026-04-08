from playwright.sync_api import sync_playwright
import re
import time
import os
import sys

os.environ["NODE_OPTIONS"] = "--openssl-legacy-provider"

HEADLESS = os.environ.get("PLAYWRIGHT_HEADLESS", "0") == "1"


def _pasta_base() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _resolver_cert(caminho: str) -> str:
    """Resolve o caminho do certificado: absoluto ou relativo a /certs."""
    if os.path.isabs(caminho) and os.path.isfile(caminho):
        return caminho
    # Tenta na pasta certs configurada ou padrão
    certs_base = os.environ.get("CERTS_PATH") or os.path.join(_pasta_base(), "certs")
    candidato = os.path.join(certs_base, os.path.basename(caminho))
    if os.path.isfile(candidato):
        return candidato
    return caminho  # devolve o original e deixa o Playwright reportar o erro


def _pasta_downloads(cnpj: str = "") -> str:
    """Pasta de downloads, organizada por CNPJ quando disponível."""
    base = (os.environ.get("DOWNLOADS_PATH")
            or _get_config_downloads()
            or os.path.join(os.path.expanduser("~"), "Downloads"))
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    pasta = os.path.join(base, cnpj_limpo) if cnpj_limpo else base
    os.makedirs(pasta, exist_ok=True)
    return pasta


def _get_config_downloads() -> str:
    return ""


def _valor_para_float(valor_fmt: str) -> float:
    return float(valor_fmt.replace(".", "").replace(",", "."))


def _float_para_br(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _formatar_valor(valor: str) -> str:
    valor = valor.strip().replace(" ", "")
    if "." in valor and "," not in valor:
        partes = valor.split(".")
        if len(partes[-1]) == 2:
            inteiro = "".join(partes[:-1])
            decimal = partes[-1]
        else:
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
    numeros = re.sub(r"\D", "", cep)
    if len(numeros) != 8:
        return False
    return 30000000 <= int(numeros) <= 31999999


def _codigo_complementar_bh(pagina, codigo_tributacao: str):
    cod = re.sub(r"\D", "", codigo_tributacao)[:6]
    pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen a").click()
    time.sleep(2)
    if cod == "171201":
        pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen").get_by_text("17.12.01.003 - Administração").click()
    elif cod == "040101":
        pagina.locator("#ServicoPrestado_CodigoComplementarMunicipal_chosen").get_by_text("- Medicina").click()


def emitir_nfse(dados: dict) -> dict:
    """
    Executa o fluxo de emissão no Portal Nacional de NFS-e via Playwright.
    Retorna {"xml": caminho_xml, "pdf": caminho_pdf}.

    Chaves esperadas em `dados`:
        caminho_certificado, senha_certificado, cep, lucro_presumido,
        tipo_doc_tomador, inscricao_tomador, sem_cep_tomador, cep_tomador,
        numero_tomador, complemento_tomador, data_competencia, local_prestacao,
        descricao_servico, valor_servico, codigo_nbs, codigo_tributacao,
        retencao_issqn, aliquota_issqn, obra, cep_obra, numero_obra,
        complemento_obra, cnpj (para organizar pasta), cliente_id (para perfil)
    """
    caminho_cert        = _resolver_cert(dados["caminho_certificado"])
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
    cnpj                = dados.get("cnpj", "")
    cliente_id          = dados.get("cliente_id", "default")

    with sync_playwright() as p:
        persist = os.environ.get("PERSIST_PATH") or _pasta_base()
        perfil = os.path.join(persist, "browser_profiles", cliente_id)
        os.makedirs(perfil, exist_ok=True)

        contexto = p.chromium.launch_persistent_context(
            user_data_dir=perfil,
            headless=HEADLESS,
            ignore_https_errors=True,
            accept_downloads=True,
            client_certificates=[{
                "origin": "https://www.nfse.gov.br",
                "pfxPath": caminho_cert,
                "passphrase": senha_cert
            }]
        )

        # Fecha guias extras restauradas da sessão anterior
        paginas_existentes = contexto.pages
        for pg in paginas_existentes[1:]:
            try:
                pg.close()
            except Exception:
                pass

        pagina = paginas_existentes[0] if paginas_existentes else contexto.new_page()

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

        # ── [4] Regime tributário ────────────────────────────────────
        if not lucro_presumido:
            print("[5] Regime tributário: Simples Nacional")
            pagina.locator("#SimplesNacional_RegimeApuracaoTributosSN_chosen a").filter(
                has_text="Selecione..."
            ).click()
            pagina.locator("#SimplesNacional_RegimeApuracaoTributosSN_chosen").get_by_text(
                "Regime de apuração dos tributos federais e municipal pelo Simples Nacional"
            ).click()

        # ── [5] CEP do emitente ──────────────────────────────────────
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
            if len(digits) <= 11:
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
        _arrow_down_municipio = {
            "ouro preto": 1, "mariana": 1, "congonhas": 1,
            "são paulo": 3, "sao paulo": 3,
        }
        _n_downs = _arrow_down_municipio.get(local.strip().lower(), 0)
        for _ in range(_n_downs):
            pagina.get_by_role("searchbox", name="Search").press("ArrowDown")
        pagina.get_by_role("searchbox", name="Search").press("Enter")

        # ── [7] Código de tributação ─────────────────────────────────
        print(f"[8] Selecionando código de tributação {codigo_tributacao}...")
        pagina.locator("#pnlServicoPrestado").get_by_label("", exact=True).click()
        time.sleep(1)
        pagina.get_by_role("searchbox", name="Search").fill(codigo_tributacao)
        time.sleep(1)
        pagina.get_by_role("searchbox", name="Search").press("Enter")

        # ── [7b] Código complementar municipal (somente BH) ─────────
        if _is_cep_bh(cep):
            print("[8b] CEP de BH detectado — preenchendo código complementar municipal...")
            _codigo_complementar_bh(pagina, codigo_tributacao)

        # ── [8] Descrição do serviço ─────────────────────────────────
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
        valor_normalizado = _formatar_valor(valor)
        print(f"[11] Valor do serviço: R$ {valor_normalizado}")
        pagina.locator("#Valores_ValorServico").fill(valor_normalizado)
        pagina.locator("body").click()

        # ── [11] Retenção de ISSQN ───────────────────────────────────
        if lucro_presumido:
            pagina.get_by_text("Não").nth(1).click()

        if retencao_issqn:
            print("[12] Retenção ISSQN: Sim")
            if lucro_presumido:
                pagina.get_by_text("Sim").nth(2).click()
            else:
                pagina.get_by_text("Sim").nth(3).click()
            pagina.get_by_text("Retido pelo Tomador").click()
            if not lucro_presumido and aliquota_issqn:
                time.sleep(5)
                pagina.locator("#ISSQN_AliquotaInformada").click()
                pagina.locator("#ISSQN_AliquotaInformada").fill(aliquota_issqn)

        if not retencao_issqn:
            print("[12] Retenção ISSQN: Não")
            pagina.get_by_text("Não").nth(3).click()

        if lucro_presumido:
            pagina.get_by_text("Não").nth(2).click()
            pagina.get_by_text("Não").nth(5).click()
        elif obra:
            pagina.get_by_text("Não").nth(5).click()
            pagina.get_by_text("Não", exact=True).nth(3).click()

        # ── [12] Tributação Federal ──────────────────────────────────
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
            pagina.locator("#TributacaoFederal_ValorCSLL").fill(_float_para_br(valor_float * (0.0065 + 0.03 + 0.01)))
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

        # ── [13] Confirmar emissão ───────────────────────────────────
        print("[14] Avançando para emissão final...")
        pagina.get_by_role("button", name="Avançar").click()
        pagina.wait_for_load_state("domcontentloaded")

        '''print("[15] Confirmando emissão...")
        pagina.locator("#btnProsseguir").click()
        pagina.wait_for_load_state("domcontentloaded")

        # ── [14] Baixar XML ──────────────────────────────────────────
        print("[16] Baixando XML...")
        pasta_dl = _pasta_downloads(cnpj)
        with pagina.expect_download() as dl_xml:
            pagina.get_by_role("link", name="Baixar XML").click()
        nome_xml = dl_xml.value.suggested_filename or "nfse.xml"
        if not nome_xml.lower().endswith(".xml"):
            nome_xml += ".xml"
        path_xml = os.path.join(pasta_dl, nome_xml)
        dl_xml.value.save_as(path_xml)
        print(f"[16] XML salvo: {path_xml}")

        # ── [15] Baixar DANFSe ───────────────────────────────────────
        print("[17] Baixando DANFSe...")
        with pagina.expect_download() as dl_pdf:
            pagina.get_by_role("link", name="Baixar DANFSe").click()
        nome_pdf = dl_pdf.value.suggested_filename or "nfse.pdf"
        if not nome_pdf.lower().endswith(".pdf"):
            nome_pdf += ".pdf"
        path_pdf = os.path.join(pasta_dl, nome_pdf)
        dl_pdf.value.save_as(path_pdf)
        print(f"[17] DANFSe salvo: {path_pdf}")'''

        contexto.close()

    #return {"xml": path_xml, "pdf": path_pdf}
