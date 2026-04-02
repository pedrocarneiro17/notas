"""
Emissão de NFS-e via API REST do Sistema Nacional NFS-e.
Usado como alternativa ao Playwright para municípios com Emissor Nacional.

Endpoint produção:        POST https://adn.nfse.gov.br/contribuintes/nfse
Endpoint prod. restrita:  POST https://adn.producaorestrita.nfse.gov.br/contribuintes/nfse

Autenticação: mTLS com certificado .pfx do contribuinte.
"""

import re
import os
import gzip
import base64
import json
import tempfile
import requests
from datetime import datetime
from lxml import etree
from signxml import XMLSigner, methods
from cryptography.hazmat.primitives.serialization import pkcs12

NS = "http://www.sped.fazenda.gov.br/nfse"
VER = "1.01"
URL_PROD          = "https://adn.nfse.gov.br/adn/DFe"
URL_PROD_RESTRITA = "https://adn.producaorestrita.nfse.gov.br/adn/DFe"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _so_numeros(v: str) -> str:
    return re.sub(r"\D", "", v or "")


def _valor_api(v: str) -> str:
    """Converte '1.500,00' ou '1500,00' para '1500.00' (ponto decimal)."""
    v = v.replace(".", "").replace(",", ".")
    return f"{float(v):.2f}"


def _data_competencia_api(data_br: str) -> str:
    """Converte 'DD/MM/YYYY' para 'YYYYMMDD'."""
    d, m, a = data_br.split("/")
    return f"{a}{m}{d}"


def _datetime_utc() -> str:
    """Retorna datetime UTC no formato exigido pela API: YYYY-MM-DDThh:mm:ss-03:00"""
    now = datetime.now()
    return now.strftime("%Y-%m-%dT%H:%M:%S") + "-03:00"


def _cod_trib_nacional(codigo_tributacao: str) -> str:
    """
    Converte código de tributação '01.07.01' para 6 dígitos numéricos '010701'.
    Formato DPS: 2 dígitos item + 2 dígitos subitem + 2 dígitos desdobro.
    """
    return _so_numeros(codigo_tributacao)[:6].zfill(6)


def _cod_nbs(codigo_nbs: str) -> str:
    """
    NBS v2.0 — formato 'X.XX.XX.XX.XX' ou só números.
    A API aceita 9 dígitos numéricos.
    """
    return _so_numeros(codigo_nbs)[:9]


# ── Construção do XML DPS ─────────────────────────────────────────────────────

def _criar_dps(dados: dict, numero_dps: int, ambiente: int = 1) -> etree._Element:
    """
    Constrói o elemento XML DPS sem assinatura.
    ambiente: 1 = produção, 2 = homologação
    """
    cnpj_emit        = _so_numeros(dados["cnpj"])
    razao_social     = dados["razao_social"]
    inscr_municipal  = dados.get("inscricao_municipal", "")
    codigo_ibge      = dados["codigo_ibge"]          # 7 dígitos IBGE
    lucro_presumido  = dados.get("lucro_presumido", False)

    tipo_doc         = dados.get("tipo_doc_tomador", "CNPJ")
    inscr_tomador    = _so_numeros(dados.get("inscricao_tomador", ""))
    sem_cep_tomador  = dados.get("sem_cep_tomador", False)

    data_competencia = _data_competencia_api(dados["data_competencia"])
    local_ibge       = dados.get("codigo_ibge_local", codigo_ibge)  # IBGE do local de prestação
    descricao        = dados["descricao_servico"]
    valor_serv       = _valor_api(dados["valor_servico"])
    cod_trib_nac     = _cod_trib_nacional(dados["codigo_tributacao"])
    cod_nbs          = _cod_nbs(dados.get("codigo_nbs", ""))
    retencao_issqn   = dados.get("retencao_issqn", False)
    aliquota_issqn   = dados.get("aliquota_issqn", "")

    E = etree.SubElement

    dps = etree.Element(f"{{{NS}}}DPS", versao=VER, nsmap={None: NS})

    inf = E(dps, f"{{{NS}}}infDPS")
    inf.set("Id", f"DPS{cnpj_emit}{data_competencia}{numero_dps:015d}")

    E(inf, f"{{{NS}}}tpAmb").text      = str(ambiente)
    E(inf, f"{{{NS}}}dhEmi").text      = _datetime_utc()
    E(inf, f"{{{NS}}}verAplic").text   = "EmissorNFSe1.0"
    E(inf, f"{{{NS}}}serie").text      = "000"
    E(inf, f"{{{NS}}}nDPS").text       = str(numero_dps)
    E(inf, f"{{{NS}}}dCompet").text    = data_competencia
    E(inf, f"{{{NS}}}tpEmit").text     = "1"   # 1 = Prestador
    E(inf, f"{{{NS}}}cLocEmi").text    = codigo_ibge

    # ── Prestador ─────────────────────────────────────────────────────────────
    prest = E(inf, f"{{{NS}}}prest")
    E(prest, f"{{{NS}}}CNPJ").text = cnpj_emit
    if inscr_municipal:
        E(prest, f"{{{NS}}}IM").text = inscr_municipal
    E(prest, f"{{{NS}}}xNome").text = razao_social

    reg_trib = E(prest, f"{{{NS}}}regTrib")
    if lucro_presumido:
        E(reg_trib, f"{{{NS}}}opSimpNac").text   = "1"   # Não optante
        E(reg_trib, f"{{{NS}}}regEspTrib").text  = "0"   # Nenhum
    else:
        E(reg_trib, f"{{{NS}}}opSimpNac").text    = "3"  # Simples Nacional ME/EPP
        E(reg_trib, f"{{{NS}}}regApTribSN").text  = "1"  # Apuração pelo SN
        E(reg_trib, f"{{{NS}}}regEspTrib").text   = "0"  # Nenhum

    # ── Tomador ───────────────────────────────────────────────────────────────
    if not sem_cep_tomador and inscr_tomador:
        toma = E(inf, f"{{{NS}}}toma")
        if tipo_doc == "CPF":
            E(toma, f"{{{NS}}}CPF").text  = inscr_tomador
        else:
            E(toma, f"{{{NS}}}CNPJ").text = inscr_tomador

    # ── Serviço ───────────────────────────────────────────────────────────────
    serv = E(inf, f"{{{NS}}}serv")

    loc_prest = E(serv, f"{{{NS}}}locPrest")
    E(loc_prest, f"{{{NS}}}cLocPrestacao").text = local_ibge

    c_serv = E(serv, f"{{{NS}}}cServ")
    E(c_serv, f"{{{NS}}}cTribNac").text  = cod_trib_nac
    E(c_serv, f"{{{NS}}}xDescServ").text = descricao
    if cod_nbs:
        E(c_serv, f"{{{NS}}}cNBS").text  = cod_nbs

    # ── Valores ───────────────────────────────────────────────────────────────
    valores = E(inf, f"{{{NS}}}valores")

    v_serv_prest = E(valores, f"{{{NS}}}vServPrest")
    E(v_serv_prest, f"{{{NS}}}vServ").text = valor_serv

    trib = E(valores, f"{{{NS}}}trib")

    trib_mun = E(trib, f"{{{NS}}}tribMun")
    E(trib_mun, f"{{{NS}}}tribISSQN").text  = "1"   # 1 = Operação tributável
    E(trib_mun, f"{{{NS}}}cLocIncid").text  = local_ibge

    if retencao_issqn and aliquota_issqn:
        aliq = aliquota_issqn.replace(",", ".")
        E(trib_mun, f"{{{NS}}}pAliq").text   = aliq
        E(trib_mun, f"{{{NS}}}tpRetISSQN").text = "1"  # 1 = Retido pelo tomador

    tot_trib = E(trib, f"{{{NS}}}totTrib")
    E(tot_trib, f"{{{NS}}}indTotTrib").text = "0"   # 0 = Não informado

    return dps


# ── Assinatura digital ────────────────────────────────────────────────────────

def _assinar_elem(elem: etree._Element, id_filho: str, pfx_path: str, pfx_senha: str) -> etree._Element:
    """Assina um elemento XML (enveloped) e retorna o elemento assinado."""
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    chave_privada, certificado, _ = pkcs12.load_key_and_certificates(
        pfx_data, pfx_senha.encode()
    )

    signer = XMLSigner(
        method=methods.enveloped,
        signature_algorithm="rsa-sha256",
        digest_algorithm="sha256",
        c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    )

    return signer.sign(
        elem,
        key=chave_privada,
        cert=[certificado],
        reference_uri="#" + id_filho,
    )


def _criar_lote_dps(dps_signed: etree._Element, dados: dict,
                    numero_dps: int, ambiente: int) -> etree._Element:
    """Envolve o DPS assinado em um LoteDPS."""
    cnpj = _so_numeros(dados["cnpj"])

    lote = etree.Element(f"{{{NS}}}LoteDPS", versao=VER, nsmap={None: NS})

    inf_lote = etree.SubElement(lote, f"{{{NS}}}infLote")
    inf_lote.set("Id", f"Lot{cnpj}{numero_dps:015d}")

    etree.SubElement(inf_lote, f"{{{NS}}}tpAmb").text    = str(ambiente)
    etree.SubElement(inf_lote, f"{{{NS}}}CNPJ").text     = cnpj
    etree.SubElement(inf_lote, f"{{{NS}}}dhEnvio").text  = _datetime_utc()
    etree.SubElement(inf_lote, f"{{{NS}}}verAplic").text = "EmissorNFSe1.0"
    etree.SubElement(inf_lote, f"{{{NS}}}qtdDPS").text   = "1"

    # DPS é filho direto do LoteDPS, não do infLote
    lote.append(dps_signed)

    return lote


# ── Envio para a API ─────────────────────────────────────────────────────────

def _extrair_pem(pfx_path: str, pfx_senha: str):
    """Extrai cert e chave do .pfx para arquivos temporários PEM (para requests mTLS)."""
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    chave, cert, chain = pkcs12.load_key_and_certificates(
        pfx_data, pfx_senha.encode()
    )

    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, NoEncryption
    )
    from cryptography.x509 import Certificate

    cert_pem  = cert.public_bytes(Encoding.PEM)
    chave_pem = chave.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

    tmp_cert  = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    tmp_chave = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    tmp_cert.write(cert_pem)
    tmp_chave.write(chave_pem)
    tmp_cert.close()
    tmp_chave.close()

    return tmp_cert.name, tmp_chave.name


def emitir_via_api(dados: dict, numero_dps: int, homologacao: bool = False) -> dict:
    """
    Emite NFS-e via API REST.
    Retorna dict com {'sucesso': bool, 'xml': str, 'erro': str}.
    """
    pfx_path = dados["caminho_certificado"]
    pfx_senha = dados["senha_certificado"]
    ambiente = 2 if homologacao else 1
    url = URL_PROD_RESTRITA if homologacao else URL_PROD

    try:
        # 1. Constrói o DPS
        dps_elem   = _criar_dps(dados, numero_dps, ambiente)
        id_dps     = dps_elem.find(f"{{{NS}}}infDPS").get("Id")

        # 2. Assina o DPS
        dps_signed = _assinar_elem(dps_elem, id_dps, pfx_path, pfx_senha)

        # 3. Envolve no LoteDPS
        lote_elem  = _criar_lote_dps(dps_signed, dados, numero_dps, ambiente)
        xml_bytes  = etree.tostring(lote_elem, xml_declaration=True, encoding="UTF-8")

        # Extrai PEM para mTLS
        cert_pem, chave_pem = _extrair_pem(pfx_path, pfx_senha)

        # GZip + Base64 conforme exigido pela API (/DFe)
        xml_gzip  = gzip.compress(xml_bytes)
        xml_b64   = base64.b64encode(xml_gzip).decode("utf-8")
        payload   = json.dumps({"LoteXmlGZipB64": [xml_b64]})

        try:
            resp = requests.post(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                cert=(cert_pem, chave_pem),
                verify=True,
                timeout=30,
            )
        finally:
            os.unlink(cert_pem)
            os.unlink(chave_pem)

        if resp.status_code == 200:
            return {"sucesso": True, "xml": resp.text, "erro": ""}
        else:
            return {"sucesso": False, "xml": "", "erro": f"HTTP {resp.status_code}: {resp.text[:500]}"}

    except Exception as e:
        return {"sucesso": False, "xml": "", "erro": str(e)}
