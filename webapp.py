from flask import Flask, render_template, request, jsonify, redirect, url_for, abort, Response, session, send_file
import threading
import uuid
import os
import re
import requests as http_requests
from functools import wraps
from datetime import datetime, timedelta, date
import calendar

import db

app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("SECRET_KEY", "nfse_webapp_2024")

with app.app_context():
    db.init_db()

WEBHOOK_URL      = os.environ.get("WEBHOOK_URL", "")
API_KEY          = os.environ.get("API_KEY", "")
TASK_ASSIGNED_TO = os.environ.get("TASK_ASSIGNED_TO", "")
TASK_CREATED_BY  = os.environ.get("TASK_CREATED_BY", "")
ADMIN_USER       = os.environ.get("ADMIN_USER", "")
ADMIN_PASS       = os.environ.get("ADMIN_PASS", "")


def _requer_login(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logado"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def _requer_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key") or request.args.get("api_key")
        if not API_KEY or key != API_KEY:
            return jsonify({"erro": "Não autorizado"}), 401
        return f(*args, **kwargs)
    return decorated


def _normalizar_cnpj(cnpj: str) -> str:
    """Remove formatação do CNPJ, deixa só números."""
    import re
    return re.sub(r"\D", "", cnpj or "")


def _formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ para XX.XXX.XXX/XXXX-XX."""
    d = _normalizar_cnpj(cnpj)
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return cnpj


def _verificar_cliente_lovable(cnpj: str, headers: dict) -> bool:
    """Retorna True se o cliente existe no Lovable pelo CNPJ."""
    try:
        resp = http_requests.post(
            WEBHOOK_URL,
            json={"action": "get_client_by_cnpj", "cnpj": cnpj},
            headers=headers,
            timeout=10,
        )
        data = resp.json()
        return bool(data.get("client", {}).get("id"))
    except Exception as e:
        print(f"[webhook] Erro ao verificar cliente: {e}")
        return False


def _disparar_webhook(pedido: dict):
    """Cria uma tarefa no sistema externo (Supabase Edge Function external-insert)."""
    if not WEBHOOK_URL or not TASK_ASSIGNED_TO or not TASK_CREATED_BY:
        return

    cliente_id  = pedido.get("cliente_id", "")
    competencia = pedido.get("data_competencia", "")
    valor       = pedido.get("valor_servico", "")
    tomador     = pedido.get("inscricao_tomador", "")

    # Busca dados do cliente no nosso banco
    cliente = db.carregar_cliente(cliente_id) or {}
    cnpj    = _formatar_cnpj(cliente.get("cnpj", ""))
    nome    = cliente_id
    print(f"[webhook] cliente_id='{cliente_id}' | cnpj_bruto='{cliente.get('cnpj','')}' | cnpj_formatado='{cnpj}'")

    due_date    = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assigned    = [u.strip() for u in TASK_ASSIGNED_TO.split(",") if u.strip()]
    assigned_to = assigned if len(assigned) > 1 else assigned[0] if assigned else TASK_ASSIGNED_TO
    headers     = {"x-api-key": API_KEY, "Content-Type": "application/json"}

    title = f"Emitir NFS-e ({competencia})"
    description = (
        f"Solicitação recebida via portal.\n"
        f"Tomador: {tomador}\n"
        f"Valor: R$ {valor}\n"
        f"Competência: {competencia}"
    )

    payload = {
        "action":      "insert_task",
        "title":       title,
        "description": description,
        "assigned_to": assigned_to,
        "created_by":  TASK_CREATED_BY,
        "type":        "fiscal",
        "priority":    "high",
        "due_date":    due_date,
    }
    if cnpj:
        payload["cnpj"] = cnpj

    def enviar():
        # Só inclui o CNPJ se o cliente já existir no Lovable
        if cnpj:
            if _verificar_cliente_lovable(cnpj, headers):
                payload["cnpj"] = cnpj
                print(f"[webhook] Cliente encontrado no Lovable, vinculando à tarefa.")
            else:
                print(f"[webhook] Cliente não encontrado no Lovable, tarefa será criada sem cliente.")

        try:
            resp = http_requests.post(
                WEBHOOK_URL,
                json=payload,
                headers=headers,
                timeout=10,
            )
            print(f"[webhook] {resp.status_code} — {resp.text}")
        except Exception as e:
            print(f"[webhook] Erro ao disparar: {e}")

    threading.Thread(target=enviar, daemon=True).start()


@app.route("/")
def index():
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    erro = None
    if request.method == "POST":
        if (request.form.get("usuario") == ADMIN_USER and
                request.form.get("senha") == ADMIN_PASS):
            session["logado"] = True
            return redirect(url_for("admin_index"))
        erro = "Usuário ou senha incorretos."
    return render_template("admin/login.html", erro=erro)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ──────────────────────────────────────────────────────────────
# CLIENTE
# ──────────────────────────────────────────────────────────────

@app.route("/pedido/<token>", methods=["GET"])
def pedido_form(token):
    info = db.get_info_token(token)
    if not info:
        abort(404)

    cliente = db.carregar_cliente(info["cliente_id"])
    if not cliente:
        abort(404)

    codigos_nbs  = cliente.get("codigos_nbs", [])
    codigos_trib = cliente.get("codigos_tributacao", [])
    tem_obra     = cliente.get("obra", False)

    hoje = date.today()
    if hoje.day <= 5:
        mes_min = hoje.month - 1 if hoje.month > 1 else 12
        ano_min = hoje.year if hoje.month > 1 else hoje.year - 1
        data_min = date(ano_min, mes_min, 1)
    else:
        data_min = date(hoje.year, hoje.month, 1)
    ultimo_dia = calendar.monthrange(hoje.year, hoje.month)[1]
    data_max = date(hoje.year, hoje.month, ultimo_dia)

    return render_template(
        "cliente/pedido.html",
        token=token,
        cliente_nome=info["cliente_id"],
        codigos_nbs=codigos_nbs,
        codigos_trib=codigos_trib,
        tem_obra=tem_obra,
        data_min=data_min.isoformat(),
        data_max=data_max.isoformat(),
    )


@app.route("/pedido/<token>", methods=["POST"])
def pedido_submit(token):
    info = db.get_info_token(token)
    if not info:
        abort(404)

    retencao = request.form.get("retencao_issqn") == "1"


    # Converte YYYY-MM-DD → DD/MM/YYYY
    data_raw = request.form.get("data_competencia", "")
    try:
        from datetime import datetime as _dt
        data_fmt = _dt.strptime(data_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        data_fmt = data_raw

    dados = {
        "tipo_doc_tomador":    request.form.get("tipo_doc_tomador", "CPF"),
        "inscricao_tomador":   request.form.get("inscricao_tomador", ""),
        "sem_cep_tomador":     request.form.get("sem_cep_tomador") == "1",
        "cep_tomador":         request.form.get("cep_tomador", ""),
        "numero_tomador":      request.form.get("numero_tomador", ""),
        "complemento_tomador": request.form.get("complemento_tomador", ""),
        "data_competencia":    data_fmt,
        "local_prestacao":     request.form.get("local_prestacao", ""),
        "descricao_servico":   request.form.get("descricao_servico", ""),
        "valor_servico":       request.form.get("valor_servico", ""),
        "codigo_nbs":          request.form.get("codigo_nbs", ""),
        "codigo_tributacao":   request.form.get("codigo_tributacao", ""),
        "retencao_issqn":      retencao,
        "aliquota_issqn":      request.form.get("aliquota_issqn", "") if retencao else "",
        "cep_obra":            request.form.get("cep_obra", ""),
        "numero_obra":         request.form.get("numero_obra", ""),
        "complemento_obra":    request.form.get("complemento_obra", ""),
    }

    pedido_id = db.criar_pedido(token, info["cliente_id"], dados)

    pedido_completo = db.get_pedido(pedido_id)
    if pedido_completo:
        _disparar_webhook(pedido_completo)

    return redirect(url_for("pedido_confirmacao", token=token))


@app.route("/pedido/<token>/confirmacao")
def pedido_confirmacao(token):
    return render_template("cliente/confirmacao.html", token=token)


# ──────────────────────────────────────────────────────────────
# ADMIN
# ──────────────────────────────────────────────────────────────

@app.route("/admin")
@_requer_login
def admin_index():
    pedidos  = db.get_pedidos()
    nomes    = db.listar_clientes()
    tokens   = sorted(db.get_tokens(), key=lambda t: t["cliente_id"].lower())
    com_token = {t["cliente_id"] for t in tokens}
    return render_template(
        "admin/index.html",
        pedidos=pedidos,
        clientes=nomes,
        tokens=tokens,
        com_token=com_token,
    )


@app.route("/admin/gerar-token", methods=["POST"])
@_requer_login
def gerar_token():
    cliente_id = request.form["cliente_id"]
    token = str(uuid.uuid4())[:8].upper()
    db.criar_token(token, cliente_id)

    return redirect(url_for("admin_index") + "#tokens")


@app.route("/admin/excluir-token/<token>", methods=["POST"])
@_requer_login
def excluir_token(token):
    db.excluir_token(token)

    return redirect(url_for("admin_index") + "#tokens")


@app.route("/admin/excluir-pedido/<int:pedido_id>", methods=["POST"])
@_requer_login
def excluir_pedido(pedido_id):
    db.excluir_pedido(pedido_id)

    return jsonify({"ok": True})


@app.route("/admin/pedidos")
@_requer_login
def pedidos_json():
    cliente_id = request.args.get("cliente_id", "")
    pedidos = db.get_pedidos(cliente_id=cliente_id if cliente_id else None)
    return jsonify(pedidos)


# ──────────────────────────────────────────────────────────────
# API REST (para sistemas externos)
# ──────────────────────────────────────────────────────────────

@app.route("/api/pedidos", methods=["GET"])
@_requer_api_key
def api_listar_pedidos():
    """Lista pedidos. Filtros: ?status=pendente&cliente_id=XXX"""
    status     = request.args.get("status")
    cliente_id = request.args.get("cliente_id")
    pedidos = db.get_pedidos(cliente_id=cliente_id, status=status)
    return jsonify(pedidos)


@app.route("/api/pedidos/<int:pedido_id>", methods=["GET"])
@_requer_api_key
def api_get_pedido(pedido_id):
    """Retorna um pedido específico."""
    pedido = db.get_pedido(pedido_id)
    if not pedido:
        return jsonify({"erro": "Pedido não encontrado"}), 404
    return jsonify(pedido)


@app.route("/api/pedidos/<int:pedido_id>/status", methods=["PATCH"])
@_requer_api_key
def api_atualizar_status(pedido_id):
    """Permite o sistema externo atualizar o status de um pedido."""
    body  = request.get_json(force=True) or {}
    novo_status = body.get("status")
    obs         = body.get("observacao", "")
    if novo_status not in ("pendente", "emitindo", "emitido", "erro"):
        return jsonify({"erro": "Status inválido"}), 400
    db.update_status(pedido_id, novo_status, obs or None)
    return jsonify({"ok": True})


# ──────────────────────────────────────────────────────────────
# GERENCIAMENTO DE CLIENTES (web)
# ──────────────────────────────────────────────────────────────

def _certs_path() -> str:
    p = os.environ.get("CERTS_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
    os.makedirs(p, exist_ok=True)
    return p


@app.route("/admin/clientes/json")
@_requer_login
def admin_clientes_json():
    clientes = db.carregar_clientes()
    return jsonify(list(clientes.values()))


@app.route("/admin/clientes/salvar", methods=["POST"])
@_requer_login
def admin_salvar_cliente():
    dados = {
        "caminho_certificado": request.form.get("caminho_certificado", ""),
        "senha_certificado":   request.form.get("senha_certificado", ""),
        "cep":                 request.form.get("cep", ""),
        "lucro_presumido":     request.form.get("lucro_presumido") == "1",
        "obra":                request.form.get("obra") == "1",
        "cnpj":                re.sub(r"\D", "", request.form.get("cnpj", "")),
        "razao_social":        request.form.get("razao_social", ""),
        "inscricao_municipal": request.form.get("inscricao_municipal", ""),
        "codigo_ibge":         request.form.get("codigo_ibge", ""),
        "codigos_nbs":         [c.strip() for c in request.form.get("codigos_nbs", "").splitlines() if c.strip()],
        "codigos_tributacao":  [c.strip() for c in request.form.get("codigos_tributacao", "").splitlines() if c.strip()],
    }
    nome = request.form.get("id", "").strip()
    if not nome:
        return jsonify({"erro": "Nome obrigatório"}), 400
    db.salvar_cliente(nome, dados)
    return jsonify({"ok": True})


@app.route("/admin/clientes/<cliente_id>/excluir", methods=["POST"])
@_requer_login
def admin_excluir_cliente(cliente_id):
    db.deletar_cliente(cliente_id)
    return jsonify({"ok": True})


@app.route("/admin/clientes/<cliente_id>/cert", methods=["POST"])
@_requer_login
def admin_upload_cert(cliente_id):
    arq = request.files.get("cert")
    if not arq or not arq.filename.lower().endswith(".pfx"):
        return jsonify({"erro": "Envie um arquivo .pfx"}), 400
    nome_arquivo = f"{cliente_id}.pfx"
    caminho = os.path.join(_certs_path(), nome_arquivo)
    arq.save(caminho)
    # Atualiza o caminho no banco
    cliente = db.carregar_cliente(cliente_id) or {}
    cliente["caminho_certificado"] = nome_arquivo
    db.salvar_cliente(cliente_id, cliente)
    return jsonify({"ok": True, "arquivo": nome_arquivo})


# ──────────────────────────────────────────────────────────────
# EMISSÃO VIA WEB
# ──────────────────────────────────────────────────────────────

def _montar_dados_emissao(pedido: dict, cliente: dict) -> dict:
    return {
        "caminho_certificado": cliente.get("caminho_certificado", ""),
        "senha_certificado":   cliente.get("senha_certificado", ""),
        "cep":                 cliente.get("cep", ""),
        "lucro_presumido":     cliente.get("lucro_presumido", False),
        "obra":                cliente.get("obra", False),
        "cnpj":                cliente.get("cnpj", ""),
        "cliente_id":          pedido.get("cliente_id", ""),
        "tipo_doc_tomador":    pedido.get("tipo_doc_tomador", "CPF"),
        "inscricao_tomador":   pedido.get("inscricao_tomador", ""),
        "sem_cep_tomador":     pedido.get("sem_cep_tomador", False),
        "cep_tomador":         pedido.get("cep_tomador", ""),
        "numero_tomador":      pedido.get("numero_tomador", ""),
        "complemento_tomador": pedido.get("complemento_tomador", ""),
        "data_competencia":    pedido.get("data_competencia", ""),
        "local_prestacao":     pedido.get("local_prestacao", ""),
        "descricao_servico":   pedido.get("descricao_servico", ""),
        "valor_servico":       pedido.get("valor_servico", ""),
        "codigo_nbs":          pedido.get("codigo_nbs", ""),
        "codigo_tributacao":   pedido.get("codigo_tributacao", ""),
        "retencao_issqn":      pedido.get("retencao_issqn", False),
        "aliquota_issqn":      pedido.get("aliquota_issqn", ""),
        "cep_obra":            pedido.get("cep_obra", ""),
        "numero_obra":         pedido.get("numero_obra", ""),
        "complemento_obra":    pedido.get("complemento_obra", ""),
    }


@app.route("/admin/emitir/<int:pedido_id>", methods=["POST"])
@_requer_login
def admin_emitir(pedido_id):
    pedido = db.get_pedido(pedido_id)
    if not pedido:
        return jsonify({"erro": "Pedido não encontrado"}), 404
    if pedido["status"] == "emitindo":
        return jsonify({"erro": "Já está emitindo"}), 400

    cliente = db.carregar_cliente(pedido["cliente_id"])
    if not cliente:
        return jsonify({"erro": "Cliente não encontrado"}), 404

    db.update_status(pedido_id, "emitindo")
    dados = _montar_dados_emissao(pedido, cliente)

    def tarefa():
        try:
            from fluxo_nfse import emitir_nfse
            resultado = emitir_nfse(dados)
            db.salvar_arquivos_pedido(pedido_id, resultado.get("xml", ""), resultado.get("pdf", ""))
            db.update_status(pedido_id, "emitido")
        except Exception as e:
            db.update_status(pedido_id, "erro", str(e))

    threading.Thread(target=tarefa, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/admin/emitir-manual", methods=["GET", "POST"])
@_requer_login
def admin_emitir_manual():
    clientes = db.listar_clientes()
    if request.method == "GET":
        hoje = date.today()
        if hoje.day <= 5:
            mes_min = hoje.month - 1 if hoje.month > 1 else 12
            ano_min = hoje.year if hoje.month > 1 else hoje.year - 1
            data_min = date(ano_min, mes_min, 1)
        else:
            data_min = date(hoje.year, hoje.month, 1)
        ultimo_dia = calendar.monthrange(hoje.year, hoje.month)[1]
        data_max = date(hoje.year, hoje.month, ultimo_dia)
        return render_template("admin/emitir_manual.html",
                               clientes=clientes,
                               data_min=data_min.isoformat(),
                               data_max=data_max.isoformat())

    # POST — cria pedido e dispara emissão
    cliente_id = request.form.get("cliente_id", "")
    cliente = db.carregar_cliente(cliente_id)
    if not cliente:
        return jsonify({"erro": "Cliente não encontrado"}), 404

    retencao = request.form.get("retencao_issqn") == "1"
    data_raw = request.form.get("data_competencia", "")
    try:
        data_fmt = datetime.strptime(data_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        data_fmt = data_raw

    dados_pedido = {
        "tipo_doc_tomador":    request.form.get("tipo_doc_tomador", "CPF"),
        "inscricao_tomador":   request.form.get("inscricao_tomador", ""),
        "sem_cep_tomador":     request.form.get("sem_cep_tomador") == "1",
        "cep_tomador":         request.form.get("cep_tomador", ""),
        "numero_tomador":      request.form.get("numero_tomador", ""),
        "complemento_tomador": request.form.get("complemento_tomador", ""),
        "data_competencia":    data_fmt,
        "local_prestacao":     request.form.get("local_prestacao", ""),
        "descricao_servico":   request.form.get("descricao_servico", ""),
        "valor_servico":       request.form.get("valor_servico", ""),
        "codigo_nbs":          request.form.get("codigo_nbs", ""),
        "codigo_tributacao":   request.form.get("codigo_tributacao", ""),
        "retencao_issqn":      retencao,
        "aliquota_issqn":      request.form.get("aliquota_issqn", "") if retencao else "",
        "cep_obra":            request.form.get("cep_obra", ""),
        "numero_obra":         request.form.get("numero_obra", ""),
        "complemento_obra":    request.form.get("complemento_obra", ""),
    }

    pedido_id = db.criar_pedido("manual", cliente_id, dados_pedido)
    db.update_status(pedido_id, "emitindo")

    dados_emissao = _montar_dados_emissao({**dados_pedido, "cliente_id": cliente_id}, cliente)

    def tarefa():
        try:
            from fluxo_nfse import emitir_nfse
            resultado = emitir_nfse(dados_emissao)
            db.salvar_arquivos_pedido(pedido_id, resultado.get("xml", ""), resultado.get("pdf", ""))
            db.update_status(pedido_id, "emitido")
        except Exception as e:
            db.update_status(pedido_id, "erro", str(e))

    threading.Thread(target=tarefa, daemon=True).start()
    return redirect(url_for("admin_index"))


@app.route("/admin/download/<int:pedido_id>/<tipo>")
@_requer_login
def admin_download_arquivo(pedido_id, tipo):
    pedido = db.get_pedido(pedido_id)
    if not pedido:
        abort(404)
    caminho = pedido.get("arquivo_xml" if tipo == "xml" else "arquivo_pdf", "")
    if not caminho or not os.path.isfile(caminho):
        abort(404)
    return send_file(caminho, as_attachment=True)


if __name__ == "__main__":
    print("Acesse o painel admin em: http://localhost:5000/admin")
    app.run(debug=False, port=5000)
