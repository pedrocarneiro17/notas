from flask import Flask, render_template, request, jsonify, redirect, url_for, abort, Response
import threading
import uuid

import db

app = Flask(__name__, template_folder="templates")
app.secret_key = "nfse_webapp_2024"

with app.app_context():
    db.init_db()


@app.route("/")
def index():
    return redirect(url_for("admin_index"))


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

    return render_template(
        "cliente/pedido.html",
        token=token,
        cliente_nome=info["cliente_id"],
        codigos_nbs=codigos_nbs,
        codigos_trib=codigos_trib,
        tem_obra=tem_obra,
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

    db.criar_pedido(token, info["cliente_id"], dados)

    return redirect(url_for("pedido_confirmacao", token=token))


@app.route("/pedido/<token>/confirmacao")
def pedido_confirmacao(token):
    return render_template("cliente/confirmacao.html", token=token)


# ──────────────────────────────────────────────────────────────
# ADMIN
# ──────────────────────────────────────────────────────────────

@app.route("/admin")
def admin_index():
    pedidos = db.get_pedidos()
    nomes   = db.listar_clientes()
    tokens  = db.get_tokens()
    return render_template(
        "admin/index.html",
        pedidos=pedidos,
        clientes=nomes,
        tokens=tokens,
    )


@app.route("/admin/gerar-token", methods=["POST"])
def gerar_token():
    cliente_id = request.form["cliente_id"]
    token = str(uuid.uuid4())[:8].upper()
    db.criar_token(token, cliente_id)

    return redirect(url_for("admin_index") + "#clientes")


@app.route("/admin/excluir-token/<token>", methods=["POST"])
def excluir_token(token):
    db.excluir_token(token)

    return redirect(url_for("admin_index") + "#clientes")


@app.route("/admin/excluir-pedido/<int:pedido_id>", methods=["POST"])
def excluir_pedido(pedido_id):
    db.excluir_pedido(pedido_id)

    return jsonify({"ok": True})


@app.route("/admin/pedidos")
def pedidos_json():
    cliente_id = request.args.get("cliente_id", "")
    pedidos = db.get_pedidos(cliente_id=cliente_id if cliente_id else None)
    return jsonify(pedidos)


if __name__ == "__main__":
    print("Acesse o painel admin em: http://localhost:5000/admin")
    app.run(debug=False, port=5000)
