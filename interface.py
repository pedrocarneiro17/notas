import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
import threading
import socket
import re
import webbrowser

from fluxo_nfse import emitir_nfse, cancelar_emissao
from db import listar_clientes, carregar_cliente, salvar_cliente, deletar_cliente, init_db


# ── Servidor Web ──────────────────────────────────────────────────────────────

def _ip_local() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def iniciar_servidor_web():
    """Sobe o Flask em background. Chamado uma única vez ao abrir o app."""
    def _run():
        from webapp import app
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True)
    t.start()


# ── Helpers de UI ────────────────────────────────────────────────────────────

def selecionar_certificado(entry_var):
    caminho = filedialog.askopenfilename(
        title="Selecionar Certificado Digital",
        filetypes=[("Certificado PFX", "*.pfx"), ("Todos os arquivos", "*.*")]
    )
    if caminho:
        entry_var.set(caminho)


def criar_label(frame, texto, linha, coluna=0, bold=False):
    fonte = ("Segoe UI", 9, "bold") if bold else ("Segoe UI", 9)
    tk.Label(frame, text=texto, font=fonte, bg="#f5f6fa", anchor="w").grid(
        row=linha, column=coluna, sticky="w", padx=(0, 8), pady=(6, 0)
    )


def criar_entry(frame, var, linha, coluna=1, largura=38, show=None):
    kwargs = {"textvariable": var, "width": largura, "font": ("Segoe UI", 9)}
    if show:
        kwargs["show"] = show
    e = ttk.Entry(frame, **kwargs)
    e.grid(row=linha, column=coluna, sticky="ew", pady=(6, 0))
    return e


def criar_separador(frame, linha, colunas=3):
    ttk.Separator(frame, orient="horizontal").grid(
        row=linha, column=0, columnspan=colunas, sticky="ew", pady=10
    )


def criar_titulo_secao(frame, texto, linha):
    tk.Label(
        frame, text=texto,
        font=("Segoe UI", 10, "bold"),
        bg="#f5f6fa", fg="#2c3e50"
    ).grid(row=linha, column=0, columnspan=3, sticky="w", pady=(10, 2))


# ── Lógica de emissão ────────────────────────────────────────────────────────

def executar_emissao(dados, btn_emitir, btn_cancelar, lbl_status):
    def _ui(fn):
        btn_emitir.after(0, fn)

    def tarefa():
        _ui(lambda: btn_emitir.config(state="disabled", text="Emitindo..."))
        _ui(lambda: btn_cancelar.config(state="normal"))
        _ui(lambda: lbl_status.config(text="⏳ Emissão em andamento...", fg="#e67e22"))
        try:
            emitir_nfse(dados)
            _ui(lambda: lbl_status.config(text="✅ NFS-e emitida com sucesso!", fg="#27ae60"))
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ("target page", "browser has been closed", "target closed",
                                      "connection closed", "page closed", "has been closed",
                                      "navigation", "crashed")):
                _ui(lambda: lbl_status.config(text="⚠️ Emissão cancelada.", fg="#7f8c8d"))
            else:
                _ui(lambda: lbl_status.config(text=f"❌ Erro: {e}", fg="#e74c3c"))
                _ui(lambda: messagebox.showerror("Erro na emissão", str(e)))
        finally:
            _ui(lambda: btn_emitir.config(state="normal", text="▶  Emitir NFS-e"))
            _ui(lambda: btn_cancelar.config(state="disabled"))

    threading.Thread(target=tarefa, daemon=True).start()


def coletar_dados(campos) -> dict:
    return {
        "caminho_certificado": campos["caminho_cert"].get().strip(),
        "senha_certificado":   campos["senha_cert"].get().strip(),
        "cep":                 campos["cep"].get().strip(),
        "lucro_presumido":     campos["lucro_presumido"].get(),
        "data_competencia":    campos["data"].get().strip(),
        "local_prestacao":     campos["local"].get().strip(),
        "descricao_servico":   campos["descricao"].get("1.0", "end").strip(),
        "valor_servico":       campos["valor"].get().strip(),
        "codigo_nbs":          campos["codigo_nbs"].get().strip(),
        "codigo_tributacao":   campos["codigo_tributacao"].get().strip(),
        "retencao_issqn":      campos["retencao_issqn"].get(),
        "aliquota_issqn":      campos["aliquota_issqn"].get().strip(),
        "tipo_doc_tomador":    campos["tipo_doc_tomador"].get(),
        "inscricao_tomador":   campos["inscricao_tomador"].get().strip(),
        "sem_cep_tomador":     campos["sem_cep_tomador"].get(),
        "cep_tomador":         campos["cep_tomador"].get().strip(),
        "numero_tomador":      campos["numero_tomador"].get().strip(),
        "complemento_tomador": campos["complemento_tomador"].get().strip(),
        "obra":                campos["obra"].get(),
        "cep_obra":            campos["cep_obra"].get().strip(),
        "numero_obra":         campos["numero_obra"].get().strip(),
        "complemento_obra":    campos["complemento_obra"].get().strip(),
    }


def validar_e_emitir(campos, btn_emitir, btn_cancelar, lbl_status):
    dados = coletar_dados(campos)

    # Re-lê dados do cliente do banco para garantir que estão atualizados
    nome_cliente = campos.get("nome_cliente_var")
    if nome_cliente and nome_cliente.get():
        cliente_atual = carregar_cliente(nome_cliente.get())
        if cliente_atual:
            dados["caminho_certificado"] = cliente_atual.get("caminho_certificado", "")
            dados["senha_certificado"]   = cliente_atual.get("senha_certificado", "")
            dados["cep"]                 = cliente_atual.get("cep", "")
            dados["lucro_presumido"]     = cliente_atual.get("lucro_presumido", False)

    obrigatorios = {
        "Caminho do Certificado": dados["caminho_certificado"],
        "Senha do Certificado":   dados["senha_certificado"],
        "CEP":                    dados["cep"],
        "Data de Competência":    dados["data_competencia"],
        "Local de Prestação":     dados["local_prestacao"],
        "Descrição do Serviço":   dados["descricao_servico"],
        "Valor do Serviço":       dados["valor_servico"],
        "Código NBS":             dados["codigo_nbs"],
        "Código de Tributação":   dados["codigo_tributacao"],
        "CPF / CNPJ do Tomador":  dados["inscricao_tomador"],
    }

    if not dados["sem_cep_tomador"]:
        obrigatorios["CEP do Tomador"]    = dados["cep_tomador"]
        obrigatorios["Número do Tomador"] = dados["numero_tomador"]

    for nome, val in obrigatorios.items():
        if not val:
            messagebox.showwarning("Campo obrigatório", f"Preencha o campo: {nome}")
            return

    if dados["retencao_issqn"] and not dados["aliquota_issqn"]:
        messagebox.showwarning("Campo obrigatório", "Informe a Alíquota do ISSQN.")
        return

    if dados["obra"]:
        if not dados["cep_obra"]:
            messagebox.showwarning("Campo obrigatório", "Informe o CEP da Obra.")
            return
        if not dados["numero_obra"]:
            messagebox.showwarning("Campo obrigatório", "Informe o Número da Obra.")
            return

    executar_emissao(dados, btn_emitir, btn_cancelar, lbl_status)


def preencher_emitente(campos, dados: dict, toggle_obra_fn=None):
    campos["caminho_cert"].set(dados.get("caminho_certificado", ""))
    campos["senha_cert"].set(dados.get("senha_certificado", ""))
    campos["cep"].set(dados.get("cep", ""))
    campos["lucro_presumido"].set(dados.get("lucro_presumido", False))
    campos["obra"].set(dados.get("obra", False))
    if toggle_obra_fn:
        toggle_obra_fn(dados.get("obra", False))


def _atualizar_combo(combo):
    combo["values"] = [""] + listar_clientes()




# ── Janela de Gerenciar Clientes ─────────────────────────────────────────────

def abrir_gerenciar_clientes(root, campos, combo_clientes, var_cliente):
    win = tk.Toplevel(root)
    win.title("Gerenciar Clientes")
    win.configure(bg="#f5f6fa")
    win.resizable(False, False)
    win.grab_set()  # modal

    # Cabeçalho da janela
    tk.Label(win, text="👥  Gerenciar Clientes",
             font=("Segoe UI", 11, "bold"), bg="#f5f6fa", fg="#2c3e50"
             ).pack(pady=(14, 4), padx=20, anchor="w")
    tk.Label(win, text="Clique em um cliente para editar",
             font=("Segoe UI", 8), bg="#f5f6fa", fg="#95a5a6"
             ).pack(padx=20, anchor="w")

    # Lista de clientes
    frame_lista = tk.Frame(win, bg="#f5f6fa")
    frame_lista.pack(padx=20, pady=(6, 0), fill="both", expand=True)

    scrollbar = tk.Scrollbar(frame_lista)
    scrollbar.pack(side="right", fill="y")

    listbox = tk.Listbox(
        frame_lista, yscrollcommand=scrollbar.set,
        font=("Segoe UI", 9), width=40, height=8,
        selectbackground="#2980b9", selectforeground="white",
        relief="solid", bd=1
    )
    listbox.pack(side="left", fill="both", expand=True)
    scrollbar.config(command=listbox.yview)

    def atualizar_lista():
        listbox.delete(0, "end")
        for nome in listar_clientes():
            listbox.insert("end", nome)

    atualizar_lista()

    # Separador
    ttk.Separator(win, orient="horizontal").pack(fill="x", padx=20, pady=10)

    # Formulário de cadastro/edição
    tk.Label(win, text="Cadastrar / Atualizar Cliente",
             font=("Segoe UI", 9, "bold"), bg="#f5f6fa", fg="#2c3e50"
             ).pack(padx=20, anchor="w")

    frame_form = tk.Frame(win, bg="#f5f6fa", padx=20)
    frame_form.pack(fill="x", pady=(4, 0))
    frame_form.columnconfigure(1, weight=1)

    def row(label, linha, var=None, show=None, largura=28):
        tk.Label(frame_form, text=label, font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=linha, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        if var is not None:
            kw = {"textvariable": var, "width": largura, "font": ("Segoe UI", 9)}
            if show:
                kw["show"] = show
            ttk.Entry(frame_form, **kw).grid(row=linha, column=1, columnspan=2,
                                              sticky="ew", pady=(6, 0))

    var_nome  = tk.StringVar()
    var_cnpj  = tk.StringVar()
    var_cert  = tk.StringVar()
    var_senha = tk.StringVar()
    var_cep   = tk.StringVar()
    var_lp    = tk.BooleanVar(value=False)
    var_obra  = tk.BooleanVar(value=False)
    nbs_vars  = [tk.StringVar()]
    trib_vars = [tk.StringVar()]

    row("Nome do cliente:", 0, var_nome)
    row("CNPJ:", 1, var_cnpj)

    # Certificado com botão buscar
    tk.Label(frame_form, text="Certificado (.pfx):", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
    ttk.Entry(frame_form, textvariable=var_cert, width=22,
              font=("Segoe UI", 9)).grid(row=2, column=1, sticky="ew", pady=(6, 0))
    ttk.Button(frame_form, text="📂",
               command=lambda: selecionar_certificado(var_cert)
               ).grid(row=2, column=2, padx=(4, 0), pady=(6, 0))

    row("Senha:", 3, var_senha, show="●")

    # CEP com máscara
    tk.Label(frame_form, text="CEP:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
    entry_cep_cli = ttk.Entry(frame_form, textvariable=var_cep, width=14, font=("Segoe UI", 9))
    entry_cep_cli.grid(row=4, column=1, sticky="w", pady=(6, 0))

    _cep_cli_lock = [False]
    def _mask_cep_cli(event=None):
        if _cep_cli_lock[0]:
            return
        teclas_ignoradas = {"BackSpace", "Delete", "Left", "Right", "Home", "End",
                            "Shift_L", "Shift_R", "Control_L", "Control_R", "Tab"}
        if event and event.keysym in teclas_ignoradas:
            return
        _cep_cli_lock[0] = True
        try:
            digits = re.sub(r"\D", "", var_cep.get())[:8]
            if len(digits) > 5:
                fmt = f"{digits[:2]}.{digits[2:5]}-{digits[5:]}"
            elif len(digits) > 2:
                fmt = f"{digits[:2]}.{digits[2:]}"
            else:
                fmt = digits
            var_cep.set(fmt)
            entry_cep_cli.icursor(tk.END)
        finally:
            _cep_cli_lock[0] = False
    entry_cep_cli.bind("<KeyRelease>", _mask_cep_cli)

    tk.Label(frame_form, text="Regime:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=5, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
    tk.Checkbutton(
        frame_form, text="Lucro Presumido  (desmarcado = Simples Nacional)",
        variable=var_lp, bg="#f5f6fa", font=("Segoe UI", 9), activebackground="#f5f6fa"
    ).grid(row=5, column=1, columnspan=2, sticky="w", pady=(6, 0))

    tk.Label(frame_form, text="Obra:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=6, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
    tk.Checkbutton(
        frame_form, text="Presta serviço para obra?",
        variable=var_obra, bg="#f5f6fa", font=("Segoe UI", 9), activebackground="#f5f6fa"
    ).grid(row=6, column=1, columnspan=2, sticky="w", pady=(6, 0))

    # ── Lista dinâmica de códigos ─────────────────────────────
    def _criar_lista_codigos(label_txt, linha, vars_list):
        tk.Label(frame_form, text=label_txt, font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=linha, column=0, sticky="nw", padx=(0, 8), pady=(8, 0))
        container = tk.Frame(frame_form, bg="#f5f6fa")
        container.grid(row=linha, column=1, columnspan=2, sticky="ew", pady=(6, 0))

        def _refresh():
            for w in container.winfo_children():
                w.destroy()
            for i, var in enumerate(vars_list):
                frm = tk.Frame(container, bg="#f5f6fa")
                frm.pack(fill="x", pady=1)
                ttk.Entry(frm, textvariable=var, width=22,
                          font=("Segoe UI", 9)).pack(side="left")
                idx = i
                ttk.Button(frm, text="×", width=2,
                           command=lambda ix=idx: _remover(ix)).pack(side="left", padx=(4, 0))
            btn_add = ttk.Button(container, text="+ Adicionar", command=_adicionar)
            btn_add.pack(anchor="w", pady=(2, 0))

        def _adicionar():
            vars_list.append(tk.StringVar())
            _refresh()

        def _remover(idx):
            if len(vars_list) <= 1:
                messagebox.showwarning("Atenção", "É necessário ao menos um código.", parent=win)
                return
            vars_list.pop(idx)
            _refresh()

        _refresh()
        return _refresh

    refresh_nbs  = _criar_lista_codigos("Códigos NBS:",      8, nbs_vars)
    refresh_trib = _criar_lista_codigos("Cód. Tributação:",  9, trib_vars)

    def preencher_form(event=None):
        sel = listbox.curselection()
        if not sel:
            return
        nome = listbox.get(sel[0])
        dados = carregar_cliente(nome)
        if dados:
            var_nome.set(nome)
            var_cnpj.set(dados.get("cnpj", ""))
            var_cert.set(dados.get("caminho_certificado", ""))
            var_senha.set(dados.get("senha_certificado", ""))
            var_cep.set(dados.get("cep", ""))
            var_lp.set(dados.get("lucro_presumido", False))
            var_obra.set(dados.get("obra", False))
            nbs_vars.clear()
            for c in dados.get("codigos_nbs", [""]):
                nbs_vars.append(tk.StringVar(value=c))
            refresh_nbs()
            trib_vars.clear()
            for c in dados.get("codigos_tributacao", [""]):
                trib_vars.append(tk.StringVar(value=c))
            refresh_trib()

    listbox.bind("<<ListboxSelect>>", preencher_form)

    def limpar_form():
        var_nome.set("")
        var_cnpj.set("")
        var_cert.set("")
        var_senha.set("")
        var_cep.set("")
        var_lp.set(False)
        var_obra.set(False)
        nbs_vars.clear()
        nbs_vars.append(tk.StringVar())
        refresh_nbs()
        trib_vars.clear()
        trib_vars.append(tk.StringVar())
        refresh_trib()
        listbox.selection_clear(0, "end")

    def salvar():
        nome = var_nome.get().strip()
        if not nome:
            messagebox.showwarning("Atenção", "Informe o nome do cliente.", parent=win)
            return
        codigos_nbs  = [v.get().strip() for v in nbs_vars  if v.get().strip()]
        codigos_trib = [v.get().strip() for v in trib_vars if v.get().strip()]
        if not codigos_nbs:
            messagebox.showwarning("Atenção", "Informe ao menos um Código NBS.", parent=win)
            return
        if not codigos_trib:
            messagebox.showwarning("Atenção", "Informe ao menos um Código de Tributação.", parent=win)
            return
        salvar_cliente(nome, {
            "cnpj":                var_cnpj.get().strip(),
            "caminho_certificado": var_cert.get().strip(),
            "senha_certificado":   var_senha.get().strip(),
            "cep":                 var_cep.get().strip(),
            "lucro_presumido":     var_lp.get(),
            "obra":                var_obra.get(),
            "codigos_nbs":         codigos_nbs,
            "codigos_tributacao":  codigos_trib,
        })
        atualizar_lista()
        _atualizar_combo(combo_clientes)
        messagebox.showinfo("Salvo", f"Cliente '{nome}' salvo!", parent=win)
        limpar_form()

    def deletar():
        sel = listbox.curselection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um cliente na lista.", parent=win)
            return
        nome = listbox.get(sel[0])
        if messagebox.askyesno("Confirmar", f"Deletar '{nome}'?", parent=win):
            deletar_cliente(nome)
            atualizar_lista()
            _atualizar_combo(combo_clientes)
            limpar_form()

    def usar_na_emissao():
        sel = listbox.curselection()
        if not sel:
            messagebox.showwarning("Atenção", "Selecione um cliente na lista.", parent=win)
            return
        nome = listbox.get(sel[0])
        dados = carregar_cliente(nome)
        if dados:
            preencher_emitente(campos, dados)
            var_cliente.set(nome)
        win.destroy()

    # Botões
    ttk.Separator(win, orient="horizontal").pack(fill="x", padx=20, pady=10)
    frame_btns = tk.Frame(win, bg="#f5f6fa", padx=20)
    frame_btns.pack(fill="x", pady=(0, 14))

    def btn(parent, texto, cor, cmd, lado="left", pad=6):
        tk.Button(
            parent, text=texto, font=("Segoe UI", 9, "bold"),
            bg=cor, fg="white", relief="flat", padx=12, pady=5,
            activebackground=cor, activeforeground="white",
            cursor="hand2", command=cmd
        ).pack(side=lado, padx=(0, pad))

    btn(frame_btns, "➕ Novo",          "#7f8c8d", limpar_form)
    btn(frame_btns, "💾 Salvar",        "#2980b9", salvar)
    btn(frame_btns, "🗑 Deletar",       "#e74c3c", deletar)
    btn(frame_btns, "✅ Usar na Emissão","#27ae60", usar_na_emissao)
    btn(frame_btns, "Fechar",           "#95a5a6", win.destroy, lado="right", pad=0)


# ── Aba Pedidos Web ───────────────────────────────────────────────────────────

def construir_aba_pedidos(parent, root):
    """Treeview com pedidos recebidos via web, com botões Autorizar/Excluir."""
    import db as _db

    # ── Info do servidor ──────────────────────────────────────
    url_admin = "https://contajur-notas.up.railway.app/admin"

    frame_srv = tk.Frame(parent, bg="#2c3e50", pady=6)
    frame_srv.pack(fill="x")

    tk.Label(
        frame_srv,
        text=f"🌐  Servidor ativo — Admin: {url_admin}  |  Clientes: https://contajur-notas.up.railway.app/pedido/<token>",
        font=("Segoe UI", 8), bg="#2c3e50", fg="#bdc3c7"
    ).pack(side="left", padx=12)

    tk.Button(
        frame_srv, text="Abrir Admin",
        font=("Segoe UI", 8), relief="flat",
        bg="#2980b9", fg="white", padx=8, pady=2, cursor="hand2",
        command=lambda: webbrowser.open(url_admin)
    ).pack(side="right", padx=8)

    STATUS_COR = {
        "pendente":  "#e67e22",
        "emitindo":  "#2980b9",
        "emitido":   "#27ae60",
        "erro":      "#e74c3c",
    }

    frame = tk.Frame(parent, bg="#f5f6fa")
    frame.pack(fill="both", expand=True, padx=16, pady=12)

    # Treeview
    cols = ("id", "cliente", "tomador", "valor", "competencia", "status")
    tree = ttk.Treeview(frame, columns=cols, show="headings", height=14)
    tree.heading("id",          text="#")
    tree.heading("cliente",     text="Cliente")
    tree.heading("tomador",     text="Tomador")
    tree.heading("valor",       text="Valor")
    tree.heading("competencia", text="Competência")
    tree.heading("status",      text="Status")
    tree.column("id",          width=40,  anchor="center")
    tree.column("cliente",     width=150)
    tree.column("tomador",     width=150)
    tree.column("valor",       width=90,  anchor="center")
    tree.column("competencia", width=100, anchor="center")
    tree.column("status",      width=90,  anchor="center")

    sb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=sb.set)
    tree.pack(side="left", fill="both", expand=True)
    sb.pack(side="right", fill="y")

    lbl_info = tk.Label(parent, text="", font=("Segoe UI", 9),
                        bg="#f5f6fa", fg="#7f8c8d")
    lbl_info.pack(pady=(4, 0))

    # Botões
    frame_btns = tk.Frame(parent, bg="#f5f6fa")
    frame_btns.pack(pady=8)

    def pedido_selecionado() -> dict | None:
        sel = tree.selection()
        if not sel:
            messagebox.showinfo("Atenção", "Selecione um pedido na lista.")
            return None
        pid = int(tree.item(sel[0])["values"][0])
        return _db.get_pedido(pid)

    def autorizar():
        p = pedido_selecionado()
        if not p:
            return
        if p["status"] not in ("pendente", "erro"):
            messagebox.showinfo("Atenção", "Apenas pedidos pendentes ou com erro podem ser autorizados.")
            return

        cliente = carregar_cliente(p["cliente_id"])
        if not cliente:
            messagebox.showerror("Erro", "Cliente não encontrado no cadastro.")
            return

        _db.update_status(p["id"], "emitindo")
        atualizar_lista()
        lbl_info.config(text=f"⏳ Emitindo pedido #{p['id']}...", fg="#e67e22")

        def tarefa():
            try:
                emitir_nfse({
                    "caminho_certificado": cliente.get("caminho_certificado", ""),
                    "senha_certificado":   cliente.get("senha_certificado", ""),
                    "cep":                 cliente.get("cep", ""),
                    "lucro_presumido":     cliente.get("lucro_presumido", False),
                    "tipo_doc_tomador":    p["tipo_doc_tomador"],
                    "inscricao_tomador":   p["inscricao_tomador"],
                    "cep_tomador":         p["cep_tomador"],
                    "numero_tomador":      p["numero_tomador"],
                    "complemento_tomador": p["complemento_tomador"] or "",
                    "data_competencia":    p["data_competencia"],
                    "local_prestacao":     p["local_prestacao"],
                    "descricao_servico":   p["descricao_servico"],
                    "valor_servico":       p["valor_servico"],
                    "codigo_nbs":          p["codigo_nbs"],
                    "codigo_tributacao":   p["codigo_tributacao"],
                    "retencao_issqn":      bool(p["retencao_issqn"]),
                    "aliquota_issqn":      p["aliquota_issqn"] or "",
                    "sem_cep_tomador":     bool(p.get("sem_cep_tomador", 0)),
                    "obra":                bool(cliente.get("obra", False)),
                    "cep_obra":            p.get("cep_obra") or "",
                    "numero_obra":         p.get("numero_obra") or "",
                    "complemento_obra":    p.get("complemento_obra") or "",
                })
                _db.update_status(p["id"], "emitido")
                root.after(0, lambda: lbl_info.config(
                    text=f"✅ Pedido #{p['id']} emitido com sucesso!", fg="#27ae60"))
            except Exception as e:
                msg = str(e).lower()
                if any(x in msg for x in ("target page", "browser has been closed",
                                           "target closed", "connection closed",
                                           "page closed", "has been closed")):
                    _db.update_status(p["id"], "pendente")
                    root.after(0, lambda: lbl_info.config(
                        text=f"⚠️ Emissão cancelada — pedido #{p['id']} voltou para pendente.",
                        fg="#7f8c8d"))
                else:
                    _db.update_status(p["id"], "erro", str(e))
                    root.after(0, lambda: lbl_info.config(
                        text=f"❌ Erro no pedido #{p['id']}: {e}", fg="#e74c3c"))
            root.after(0, atualizar_lista)

        threading.Thread(target=tarefa, daemon=True).start()

    def excluir():
        p = pedido_selecionado()
        if not p:
            return
        if messagebox.askyesno("Excluir", f"Excluir pedido #{p['id']}? Esta ação não pode ser desfeita."):
            _db.excluir_pedido(p["id"])
            atualizar_lista()

    def editar():
        p = pedido_selecionado()
        if not p:
            return

        win = tk.Toplevel(root)
        win.title(f"Editar Pedido #{p['id']}")
        win.configure(bg="#f5f6fa")
        win.resizable(False, False)
        win.grab_set()

        tk.Label(win, text=f"✏️  Editar Pedido #{p['id']} — {p['cliente_id']}",
                 font=("Segoe UI", 11, "bold"), bg="#f5f6fa", fg="#2c3e50"
                 ).pack(pady=(14, 4), padx=20, anchor="w")

        frm = tk.Frame(win, bg="#f5f6fa", padx=20, pady=8)
        frm.pack(fill="x")
        frm.columnconfigure(1, weight=1)

        def campo(label, linha, valor, largura=30):
            tk.Label(frm, text=label, font=("Segoe UI", 9),
                     bg="#f5f6fa").grid(row=linha, column=0, sticky="w",
                                        padx=(0, 8), pady=(6, 0))
            v = tk.StringVar(value=valor or "")
            ttk.Entry(frm, textvariable=v, width=largura,
                      font=("Segoe UI", 9)).grid(row=linha, column=1,
                                                  sticky="ew", pady=(6, 0))
            return v

        cliente_data = carregar_cliente(p["cliente_id"]) or {}
        codigos_nbs  = cliente_data.get("codigos_nbs", [p["codigo_nbs"]])
        codigos_trib = cliente_data.get("codigos_tributacao", [p["codigo_tributacao"]])

        tem_obra = bool(cliente_data.get("obra", False))

        # ── Tomador ──────────────────────────────────────────────
        tk.Label(frm, text="── Tomador ──", font=("Segoe UI", 8, "bold"),
                 fg="#7f8c8d", bg="#f5f6fa").grid(row=0, column=0, columnspan=2,
                                                   sticky="w", pady=(0, 2))

        v_tipo  = campo("Tipo Doc:",    1, p["tipo_doc_tomador"], 8)
        v_inscr = campo("CPF/CNPJ:",   2, p["inscricao_tomador"])

        v_sem_cep = tk.BooleanVar(value=bool(p.get("sem_cep_tomador", 0)))
        tk.Label(frm, text="Sem CEP:", font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        tk.Checkbutton(frm, variable=v_sem_cep, bg="#f5f6fa",
                       command=lambda: _toggle_sem_cep()
                       ).grid(row=3, column=1, sticky="w", pady=(6, 0))

        v_cep_tom  = campo("CEP Tomador:",  4, p["cep_tomador"], 14)
        v_num_tom  = campo("Número:",       5, p["numero_tomador"], 10)
        v_comp_tom = campo("Complemento:",  6, p["complemento_tomador"], 20)

        entries_cep = []
        for r in (4, 5, 6):
            for w in frm.grid_slaves(row=r, column=1):
                entries_cep.append(w)

        def _toggle_sem_cep():
            estado = "disabled" if v_sem_cep.get() else "normal"
            for w in entries_cep:
                w.config(state=estado)

        _toggle_sem_cep()

        # ── Serviço ──────────────────────────────────────────────
        tk.Label(frm, text="── Serviço ──", font=("Segoe UI", 8, "bold"),
                 fg="#7f8c8d", bg="#f5f6fa").grid(row=7, column=0, columnspan=2,
                                                   sticky="w", pady=(8, 2))

        v_data  = campo("Competência:",     8,  p["data_competencia"], 14)
        v_local = campo("Local Prestação:", 9,  p["local_prestacao"])
        v_valor = campo("Valor (R$):",      10, p["valor_servico"], 14)

        tk.Label(frm, text="Descrição:", font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=11, column=0, sticky="nw", padx=(0, 8), pady=(6, 0))
        txt_desc = tk.Text(frm, width=30, height=3, font=("Segoe UI", 9), relief="solid", bd=1)
        txt_desc.grid(row=11, column=1, sticky="ew", pady=(6, 0))
        txt_desc.insert("1.0", p["descricao_servico"] or "")

        # ── Códigos ──────────────────────────────────────────────
        tk.Label(frm, text="Cód. Tributação:", font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=12, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        v_trib = tk.StringVar(value=p["codigo_tributacao"] or "")
        ttk.Combobox(frm, textvariable=v_trib, values=codigos_trib,
                     state="readonly", width=28, font=("Segoe UI", 9)
                     ).grid(row=12, column=1, sticky="w", pady=(6, 0))

        tk.Label(frm, text="Código NBS:", font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=13, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        v_nbs = tk.StringVar(value=p["codigo_nbs"] or "")
        ttk.Combobox(frm, textvariable=v_nbs, values=codigos_nbs,
                     state="readonly", width=28, font=("Segoe UI", 9)
                     ).grid(row=13, column=1, sticky="w", pady=(6, 0))

        # ── Retenção ISSQN ───────────────────────────────────────
        v_ret  = tk.BooleanVar(value=bool(p["retencao_issqn"]))
        v_aliq = tk.StringVar(value=p["aliquota_issqn"] or "")

        tk.Label(frm, text="Retenção ISSQN:", font=("Segoe UI", 9),
                 bg="#f5f6fa").grid(row=14, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        frm_ret = tk.Frame(frm, bg="#f5f6fa")
        frm_ret.grid(row=14, column=1, sticky="w", pady=(6, 0))
        tk.Checkbutton(frm_ret, text="Sim", variable=v_ret,
                       bg="#f5f6fa", font=("Segoe UI", 9),
                       command=lambda: _toggle_aliq()).pack(side="left")
        lbl_aliq_ed  = tk.Label(frm_ret, text="Alíquota (%):", font=("Segoe UI", 9), bg="#f5f6fa")
        entry_aliq_ed = ttk.Entry(frm_ret, textvariable=v_aliq, width=8, font=("Segoe UI", 9))

        def _toggle_aliq():
            if v_ret.get():
                lbl_aliq_ed.pack(side="left", padx=(10, 4))
                entry_aliq_ed.pack(side="left")
            else:
                lbl_aliq_ed.pack_forget()
                entry_aliq_ed.pack_forget()
        _toggle_aliq()

        # ── Obra (só se cliente tiver flag) ──────────────────────
        v_cep_obra  = tk.StringVar(value=p.get("cep_obra") or "")
        v_num_obra  = tk.StringVar(value=p.get("numero_obra") or "")
        v_comp_obra = tk.StringVar(value=p.get("complemento_obra") or "")

        if tem_obra:
            tk.Label(frm, text="── Obra ──", font=("Segoe UI", 8, "bold"),
                     fg="#7f8c8d", bg="#f5f6fa").grid(row=15, column=0, columnspan=2,
                                                       sticky="w", pady=(8, 2))
            campo("CEP da Obra:",    16, "")
            campo("Número Obra:",    17, "")
            campo("Compl. Obra:",    18, "")
            # substituir vars nas entries criadas
            for r, var in ((16, v_cep_obra), (17, v_num_obra), (18, v_comp_obra)):
                for w in frm.grid_slaves(row=r, column=1):
                    w.config(textvariable=var)

        def salvar_edicao():
            erros = []
            if not v_inscr.get().strip():
                erros.append("CPF / CNPJ do Tomador")
            if not v_sem_cep.get():
                if not v_cep_tom.get().strip():
                    erros.append("CEP do Tomador")
                if not v_num_tom.get().strip():
                    erros.append("Número do Tomador")
            if not v_data.get().strip():
                erros.append("Competência")
            if not v_local.get().strip():
                erros.append("Local de Prestação")
            if not txt_desc.get("1.0", "end").strip():
                erros.append("Descrição")
            if not v_valor.get().strip():
                erros.append("Valor")
            if not v_nbs.get().strip():
                erros.append("Código NBS")
            if not v_trib.get().strip():
                erros.append("Código de Tributação")
            if v_ret.get() and not v_aliq.get().strip():
                erros.append("Alíquota ISSQN")
            if tem_obra:
                if not v_cep_obra.get().strip():
                    erros.append("CEP da Obra")
                if not v_num_obra.get().strip():
                    erros.append("Número da Obra")
            if erros:
                messagebox.showwarning("Campos obrigatórios",
                                       "Preencha: " + ", ".join(erros), parent=win)
                return

            dados = {
                "tipo_doc_tomador":    v_tipo.get().strip(),
                "inscricao_tomador":   v_inscr.get().strip(),
                "sem_cep_tomador":     v_sem_cep.get(),
                "cep_tomador":         v_cep_tom.get().strip(),
                "numero_tomador":      v_num_tom.get().strip(),
                "complemento_tomador": v_comp_tom.get().strip(),
                "data_competencia":    v_data.get().strip(),
                "local_prestacao":     v_local.get().strip(),
                "descricao_servico":   txt_desc.get("1.0", "end").strip(),
                "valor_servico":       v_valor.get().strip(),
                "codigo_nbs":          v_nbs.get().strip(),
                "codigo_tributacao":   v_trib.get().strip(),
                "retencao_issqn":      v_ret.get(),
                "aliquota_issqn":      v_aliq.get().strip(),
                "cep_obra":            v_cep_obra.get().strip(),
                "numero_obra":         v_num_obra.get().strip(),
                "complemento_obra":    v_comp_obra.get().strip(),
            }
            _db.atualizar_pedido(p["id"], dados)
            atualizar_lista()
            win.destroy()

        frame_btns_edit = tk.Frame(win, bg="#f5f6fa")
        frame_btns_edit.pack(pady=12)
        tk.Button(frame_btns_edit, text="💾 Salvar", font=("Segoe UI", 9, "bold"),
                  bg="#2980b9", fg="white", relief="flat", padx=14, pady=6,
                  cursor="hand2", command=salvar_edicao).pack(side="left", padx=4)
        tk.Button(frame_btns_edit, text="Fechar", font=("Segoe UI", 9),
                  bg="#95a5a6", fg="white", relief="flat", padx=14, pady=6,
                  cursor="hand2", command=win.destroy).pack(side="left", padx=4)

    def atualizar_lista():
        pedidos = _db.get_pedidos()
        tree.delete(*tree.get_children())
        for p in pedidos:
            status = p["status"]
            tree.insert("", "end", values=(
                p["id"],
                p["cliente_id"],
                p["inscricao_tomador"],
                f"R$ {p['valor_servico']}",
                p["data_competencia"],
                status.upper(),
            ), tags=(status,))
        for s, cor in STATUS_COR.items():
            tree.tag_configure(s, foreground=cor)

    for txt, cor, cmd in [
        ("✓  Autorizar", "#27ae60", autorizar),
        ("✏️  Editar",   "#8e44ad", editar),
        ("🗑  Excluir",  "#e74c3c", excluir),
        ("↺  Atualizar", "#2980b9", atualizar_lista),
    ]:
        tk.Button(
            frame_btns, text=txt, font=("Segoe UI", 9, "bold"),
            bg=cor, fg="white", relief="flat", padx=14, pady=6,
            activebackground=cor, cursor="hand2", command=cmd
        ).pack(side="left", padx=4)

    atualizar_lista()

    # Auto-refresh a cada 5 segundos
    def _auto_refresh():
        atualizar_lista()
        root.after(5000, _auto_refresh)
    root.after(5000, _auto_refresh)


# ── Interface principal ──────────────────────────────────────────────────────

def main():
    init_db()
    root = tk.Tk()
    root.title("Emissor NFS-e — Portal Nacional")
    root.configure(bg="#f5f6fa")
    root.resizable(True, True)
    root.minsize(700, 520)

    cab = tk.Frame(root, bg="#2c3e50", pady=14)
    cab.pack(fill="x")
    tk.Label(cab, text="🧾  Emissor NFS-e", font=("Segoe UI", 14, "bold"),
             bg="#2c3e50", fg="white").pack()
    tk.Label(cab, text="Portal Nacional de Notas Fiscais de Serviço Eletrônicas",
             font=("Segoe UI", 8), bg="#2c3e50", fg="#bdc3c7").pack()

    # ── Notebook (abas) ───────────────────────────────────────
    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True, padx=0, pady=0)

    aba_pedidos = tk.Frame(nb, bg="#f5f6fa")
    aba_manual  = tk.Frame(nb, bg="#f5f6fa")
    nb.add(aba_pedidos, text="  📋 Pedidos  ")
    nb.add(aba_manual,  text="  ✏️ Emissão Manual  ")

    construir_aba_pedidos(aba_pedidos, root)

    # A aba manual recebe o frame original
    frame = tk.Frame(aba_manual, bg="#f5f6fa", padx=24, pady=16)
    frame.pack(fill="both", expand=True)
    frame.columnconfigure(1, weight=1)

    campos = {}

    # ── Seleção de cliente ────────────────────────────────────────
    # Edição dos dados do emitente disponível apenas em "Gerenciar Clientes"
    tk.Label(frame, text="Cliente:", font=("Segoe UI", 9),
             bg="#f5f6fa", anchor="w").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0))

    var_cliente = tk.StringVar()
    campos["nome_cliente_var"] = var_cliente
    combo_clientes = ttk.Combobox(
        frame, textvariable=var_cliente,
        values=[""] + listar_clientes(),
        state="readonly", width=30, font=("Segoe UI", 9)
    )
    combo_clientes.grid(row=2, column=1, sticky="w", pady=(6, 0))

    # Inicializa vars ocultas do emitente (usadas pelo fluxo mas não exibidas)
    campos["caminho_cert"]   = tk.StringVar()
    campos["senha_cert"]     = tk.StringVar()
    campos["cep"]            = tk.StringVar()
    campos["lucro_presumido"] = tk.BooleanVar(value=False)

    def ao_selecionar(event=None):
        nome = var_cliente.get()
        if nome:
            dados = carregar_cliente(nome)
            if dados:
                preencher_emitente(campos, dados, toggle_obra)

                codigos_nbs = dados.get("codigos_nbs", [])
                combo_nbs["values"] = codigos_nbs
                campos["codigo_nbs"].set(codigos_nbs[0] if len(codigos_nbs) == 1 else "")

                codigos_trib = dados.get("codigos_tributacao", [])
                combo_trib["values"] = codigos_trib
                campos["codigo_tributacao"].set(codigos_trib[0] if len(codigos_trib) == 1 else "")

    combo_clientes.bind("<<ComboboxSelected>>", ao_selecionar)

    ttk.Button(
        frame, text="⚙ Gerenciar Clientes",
        command=lambda: abrir_gerenciar_clientes(root, campos, combo_clientes, var_cliente)
    ).grid(row=2, column=2, padx=(6, 0), pady=(6, 0))

    # ── SEÇÃO 2: Dados da Nota ────────────────────────────────────
    criar_titulo_secao(frame, "📋  Dados da Nota Fiscal", 3)
    criar_separador(frame, 4)

    campos["data"] = tk.StringVar(value=datetime.now().strftime("%d/%m/%Y"))
    criar_label(frame, "Data de Competência:", 5)
    criar_entry(frame, campos["data"], 5, largura=14)

    campos["local"] = tk.StringVar(value="Ouro Preto")
    criar_label(frame, "Município de Prestação:", 6)
    criar_entry(frame, campos["local"], 6)

    criar_label(frame, "Descrição do Serviço:", 7)
    campos["descricao"] = tk.Text(frame, width=38, height=3,
                                  font=("Segoe UI", 9), relief="solid", bd=1)
    campos["descricao"].grid(row=7, column=1, columnspan=2, sticky="ew", pady=(6, 0))

    campos["codigo_tributacao"] = tk.StringVar()
    criar_label(frame, "Cód. Tributação:", 8)
    combo_trib = ttk.Combobox(frame, textvariable=campos["codigo_tributacao"],
                               state="readonly", width=20, font=("Segoe UI", 9))
    combo_trib.grid(row=8, column=1, sticky="w", pady=(6, 0))

    campos["codigo_nbs"] = tk.StringVar()
    criar_label(frame, "Código NBS:", 9)
    combo_nbs = ttk.Combobox(frame, textvariable=campos["codigo_nbs"],
                              state="readonly", width=20, font=("Segoe UI", 9))
    combo_nbs.grid(row=9, column=1, sticky="w", pady=(6, 0))

    campos["valor"] = tk.StringVar()
    criar_label(frame, "Valor do Serviço (R$):", 10)
    entry_valor = criar_entry(frame, campos["valor"], 10, largura=16)

    _valor_lock = [False]
    def _mask_valor(event=None):
        if _valor_lock[0]:
            return
        teclas_ignoradas = {"BackSpace", "Delete", "Left", "Right", "Home", "End",
                            "Shift_L", "Shift_R", "Control_L", "Control_R", "Tab"}
        if event and event.keysym in teclas_ignoradas:
            return
        _valor_lock[0] = True
        try:
            v = campos["valor"].get()
            partes = v.split(",")
            inteiro  = re.sub(r"\D", "", partes[0])
            centavos = re.sub(r"\D", "", partes[1])[:2] if len(partes) > 1 else ""
            if inteiro:
                inteiro = str(int(inteiro))          # remove zeros à esquerda
                # separadores de milhar
                grupos = []
                while len(inteiro) > 3:
                    grupos.insert(0, inteiro[-3:])
                    inteiro = inteiro[:-3]
                grupos.insert(0, inteiro)
                inteiro = ".".join(grupos)
            fmt = f"{inteiro},{centavos}" if "," in v else inteiro
            campos["valor"].set(fmt)
            entry_valor.icursor(tk.END)
        finally:
            _valor_lock[0] = False

    entry_valor.bind("<KeyRelease>", _mask_valor)
    tk.Label(frame, text="Ex: 2.000,00", font=("Segoe UI", 8),
             fg="#95a5a6", bg="#f5f6fa").grid(row=10, column=2, sticky="w",
                                               padx=(6, 0), pady=(6, 0))

    # ── Retenção de ISSQN ─────────────────────────────────────────
    campos["retencao_issqn"] = tk.BooleanVar(value=False)
    campos["aliquota_issqn"] = tk.StringVar()

    frame_issqn = tk.Frame(frame, bg="#f5f6fa")
    frame_issqn.grid(row=11, column=0, columnspan=3, sticky="ew", pady=(6, 0))

    tk.Checkbutton(
        frame_issqn, text="Retenção de ISSQN?",
        variable=campos["retencao_issqn"],
        bg="#f5f6fa", font=("Segoe UI", 9), activebackground="#f5f6fa"
    ).pack(side="left")

    lbl_aliquota = tk.Label(frame_issqn, text="  Alíquota (%):", font=("Segoe UI", 9), bg="#f5f6fa")
    entry_aliquota = ttk.Entry(frame_issqn, textvariable=campos["aliquota_issqn"],
                               width=8, font=("Segoe UI", 9))

    def toggle_issqn(*args):
        if campos["retencao_issqn"].get():
            lbl_aliquota.pack(side="left")
            entry_aliquota.pack(side="left", padx=(0, 4))
        else:
            lbl_aliquota.pack_forget()
            entry_aliquota.pack_forget()

    campos["retencao_issqn"].trace_add("write", toggle_issqn)

    # ── Tomador ───────────────────────────────────────────────────
    criar_titulo_secao(frame, "👤  Tomador do Serviço", 12)
    criar_separador(frame, 13)

    campos["tipo_doc_tomador"] = tk.StringVar(value="CPF")
    frame_tipo = tk.Frame(frame, bg="#f5f6fa")
    frame_tipo.grid(row=14, column=0, columnspan=3, sticky="w", pady=(6, 0))
    tk.Label(frame_tipo, text="Tipo de documento:", font=("Segoe UI", 9),
             bg="#f5f6fa").pack(side="left", padx=(0, 8))
    tk.Radiobutton(frame_tipo, text="CPF", variable=campos["tipo_doc_tomador"],
                   value="CPF", bg="#f5f6fa", font=("Segoe UI", 9),
                   activebackground="#f5f6fa").pack(side="left")
    tk.Radiobutton(frame_tipo, text="CNPJ", variable=campos["tipo_doc_tomador"],
                   value="CNPJ", bg="#f5f6fa", font=("Segoe UI", 9),
                   activebackground="#f5f6fa").pack(side="left", padx=(8, 0))

    campos["inscricao_tomador"] = tk.StringVar()
    criar_label(frame, "CPF / CNPJ:", 15)
    entry_inscricao = criar_entry(frame, campos["inscricao_tomador"], 15, largura=22)

    _insc_lock = [False]
    def _mask_inscricao(event=None):
        if _insc_lock[0]:
            return
        teclas_ignoradas = {"BackSpace", "Delete", "Left", "Right", "Home", "End",
                            "Shift_L", "Shift_R", "Control_L", "Control_R", "Tab"}
        if event and event.keysym in teclas_ignoradas:
            return
        _insc_lock[0] = True
        try:
            digits = re.sub(r"\D", "", campos["inscricao_tomador"].get())
            tipo = campos["tipo_doc_tomador"].get()
            if tipo == "CPF":
                digits = digits[:11]
                if len(digits) > 9:
                    fmt = f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
                elif len(digits) > 6:
                    fmt = f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"
                elif len(digits) > 3:
                    fmt = f"{digits[:3]}.{digits[3:]}"
                else:
                    fmt = digits
            else:
                digits = digits[:14]
                if len(digits) > 12:
                    fmt = f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
                elif len(digits) > 8:
                    fmt = f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:]}"
                elif len(digits) > 5:
                    fmt = f"{digits[:2]}.{digits[2:5]}.{digits[5:]}"
                elif len(digits) > 2:
                    fmt = f"{digits[:2]}.{digits[2:]}"
                else:
                    fmt = digits
            campos["inscricao_tomador"].set(fmt)
            entry_inscricao.icursor(tk.END)
        finally:
            _insc_lock[0] = False

    entry_inscricao.bind("<KeyRelease>", _mask_inscricao)

    campos["sem_cep_tomador"] = tk.BooleanVar(value=False)
    criar_label(frame, "CEP do Tomador:", 16)
    frm_cep = tk.Frame(frame, bg="#f5f6fa")
    frm_cep.grid(row=16, column=1, sticky="w")
    campos["cep_tomador"] = tk.StringVar()
    entry_cep_tom = tk.Entry(frm_cep, textvariable=campos["cep_tomador"], width=14,
                             font=("Segoe UI", 10), relief="solid", bd=1)
    entry_cep_tom.pack(side="left")

    _cep_lock = [False]
    def _mask_cep(event=None):
        if _cep_lock[0]:
            return
        teclas_ignoradas = {"BackSpace", "Delete", "Left", "Right", "Home", "End",
                            "Shift_L", "Shift_R", "Control_L", "Control_R", "Tab"}
        if event and event.keysym in teclas_ignoradas:
            return
        _cep_lock[0] = True
        try:
            digits = re.sub(r"\D", "", campos["cep_tomador"].get())[:8]
            if len(digits) > 5:
                fmt = f"{digits[:2]}.{digits[2:5]}-{digits[5:]}"
            elif len(digits) > 2:
                fmt = f"{digits[:2]}.{digits[2:]}"
            else:
                fmt = digits
            campos["cep_tomador"].set(fmt)
            entry_cep_tom.icursor(tk.END)
        finally:
            _cep_lock[0] = False

    entry_cep_tom.bind("<KeyRelease>", _mask_cep)

    chk_sem_cep = tk.Checkbutton(frm_cep, text="Sem CEP",
                                  variable=campos["sem_cep_tomador"],
                                  bg="#f5f6fa", font=("Segoe UI", 9),
                                  command=lambda: _toggle_cep_tomador())
    chk_sem_cep.pack(side="left", padx=(8, 0))

    campos["numero_tomador"] = tk.StringVar()
    criar_label(frame, "Número:", 17)
    entry_num_tom = criar_entry(frame, campos["numero_tomador"], 17, largura=10)

    campos["complemento_tomador"] = tk.StringVar()
    criar_label(frame, "Complemento:", 18)
    entry_comp_tom = criar_entry(frame, campos["complemento_tomador"], 18, largura=16)
    tk.Label(frame, text="opcional", font=("Segoe UI", 8),
             fg="#95a5a6", bg="#f5f6fa").grid(row=18, column=2, sticky="w",
                                               padx=(6, 0), pady=(6, 0))

    def _toggle_cep_tomador():
        estado = "disabled" if campos["sem_cep_tomador"].get() else "normal"
        entry_cep_tom.config(state=estado)
        entry_num_tom.config(state=estado)
        entry_comp_tom.config(state=estado)

    # ── Obra (visível apenas se cliente tiver flag obra=True) ─────
    campos["obra"] = tk.BooleanVar(value=False)
    campos["cep_obra"] = tk.StringVar()
    campos["numero_obra"] = tk.StringVar()
    campos["complemento_obra"] = tk.StringVar()

    frame_obra_campos = tk.Frame(frame, bg="#f5f6fa")
    lbl_obra_sec = tk.Label(frame_obra_campos, text="📍  Endereço da Obra",
                            font=("Segoe UI", 9, "bold"), bg="#f5f6fa", fg="#2c3e50")
    lbl_obra_sec.grid(row=0, column=0, columnspan=3, sticky="w", pady=(4, 2))

    tk.Label(frame_obra_campos, text="CEP da Obra:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(4, 0))
    ttk.Entry(frame_obra_campos, textvariable=campos["cep_obra"],
              width=14, font=("Segoe UI", 9)).grid(row=1, column=1, sticky="w", pady=(4, 0))

    tk.Label(frame_obra_campos, text="Número:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(4, 0))
    ttk.Entry(frame_obra_campos, textvariable=campos["numero_obra"],
              width=10, font=("Segoe UI", 9)).grid(row=2, column=1, sticky="w", pady=(4, 0))

    tk.Label(frame_obra_campos, text="Complemento:", font=("Segoe UI", 9),
             bg="#f5f6fa").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=(4, 0))
    ttk.Entry(frame_obra_campos, textvariable=campos["complemento_obra"],
              width=16, font=("Segoe UI", 9)).grid(row=3, column=1, sticky="w", pady=(4, 0))
    tk.Label(frame_obra_campos, text="opcional", font=("Segoe UI", 8),
             fg="#95a5a6", bg="#f5f6fa").grid(row=3, column=2, sticky="w", padx=(6, 0))

    def toggle_obra(ativo: bool):
        if ativo:
            frame_obra_campos.grid(row=20, column=0, columnspan=3, sticky="ew", pady=(0, 4))
        else:
            campos["cep_obra"].set("")
            campos["numero_obra"].set("")
            campos["complemento_obra"].set("")
            frame_obra_campos.grid_remove()

    # ── Rodapé ────────────────────────────────────────────────────
    criar_separador(frame, 21)

    lbl_status = tk.Label(frame, text="", font=("Segoe UI", 9),
                          bg="#f5f6fa", fg="#7f8c8d")
    lbl_status.grid(row=22, column=0, columnspan=3, sticky="w", pady=(0, 6))

    frm_btns = tk.Frame(frame, bg="#f5f6fa")
    frm_btns.grid(row=23, column=0, columnspan=3, pady=(4, 8))

    btn_emitir = tk.Button(
        frm_btns, text="▶  Emitir NFS-e",
        font=("Segoe UI", 10, "bold"),
        bg="#27ae60", fg="white",
        activebackground="#219a52", activeforeground="white",
        relief="flat", padx=20, pady=8, cursor="hand2",
    )
    btn_emitir.pack(side="left", padx=(0, 8))

    btn_cancelar = tk.Button(
        frm_btns, text="⛔  Cancelar",
        font=("Segoe UI", 10, "bold"),
        bg="#e74c3c", fg="white",
        activebackground="#c0392b", activeforeground="white",
        relief="flat", padx=20, pady=8, cursor="hand2",
        state="disabled",
        command=cancelar_emissao,
    )
    btn_cancelar.pack(side="left")

    btn_emitir.config(command=lambda: validar_e_emitir(campos, btn_emitir, btn_cancelar, lbl_status))

    root.mainloop()


if __name__ == "__main__":
    iniciar_servidor_web()
    main()