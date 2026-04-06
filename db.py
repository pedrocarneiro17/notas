"""
Camada de banco de dados unificada — PostgreSQL (Railway).
Substitui clientes.py (JSON) e webapp_db.py (SQLite).
"""
import os
import sys
import json
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Carrega .env ao lado do .exe (PyInstaller) ou do script
if getattr(sys, "frozen", False):
    _base = os.path.dirname(sys.executable)
else:
    _base = os.path.dirname(os.path.abspath(__file__))

load_dotenv(os.path.join(_base, ".env"))


def _conn():
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PUBLIC_URL")
    if not url:
        raise RuntimeError("Variável DATABASE_URL ou DATABASE_PUBLIC_URL não definida.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def _row(cursor):
    row = cursor.fetchone()
    return dict(row) if row else None


def _rows(cursor):
    return [dict(r) for r in cursor.fetchall()]


# ── Inicialização ──────────────────────────────────────────────────────────────

def init_db():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes (
                    id                  TEXT PRIMARY KEY,
                    caminho_certificado TEXT DEFAULT '',
                    senha_certificado   TEXT DEFAULT '',
                    cep                 TEXT DEFAULT '',
                    lucro_presumido     BOOLEAN DEFAULT FALSE,
                    obra                BOOLEAN DEFAULT FALSE,
                    codigos_nbs         JSONB DEFAULT '[]',
                    codigos_tributacao  JSONB DEFAULT '[]',
                    cnpj                TEXT DEFAULT '',
                    razao_social        TEXT DEFAULT '',
                    inscricao_municipal TEXT DEFAULT '',
                    codigo_ibge         TEXT DEFAULT '',
                    numero_dps          INTEGER DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS tokens (
                    token      TEXT PRIMARY KEY,
                    cliente_id TEXT NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
                    ativo      BOOLEAN DEFAULT TRUE
                );

                CREATE TABLE IF NOT EXISTS pedidos (
                    id                  SERIAL PRIMARY KEY,
                    token               TEXT,
                    cliente_id          TEXT,
                    tipo_doc_tomador    TEXT,
                    inscricao_tomador   TEXT,
                    sem_cep_tomador     BOOLEAN DEFAULT FALSE,
                    cep_tomador         TEXT,
                    numero_tomador      TEXT,
                    complemento_tomador TEXT,
                    data_competencia    TEXT,
                    local_prestacao     TEXT,
                    descricao_servico   TEXT,
                    valor_servico       TEXT,
                    codigo_tributacao   TEXT,
                    codigo_nbs          TEXT,
                    retencao_issqn      BOOLEAN DEFAULT FALSE,
                    aliquota_issqn      TEXT,
                    cep_obra            TEXT,
                    numero_obra         TEXT,
                    complemento_obra    TEXT,
                    status              TEXT DEFAULT 'pendente',
                    observacao          TEXT
                );
            """)
        conn.commit()

    # Migrações
    with _conn() as conn:
        with conn.cursor() as cur:
            for sql in [
                "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS cnpj TEXT DEFAULT ''",
                "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS razao_social TEXT DEFAULT ''",
                "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS inscricao_municipal TEXT DEFAULT ''",
                "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS codigo_ibge TEXT DEFAULT ''",
                "ALTER TABLE clientes ADD COLUMN IF NOT EXISTS numero_dps INTEGER DEFAULT 1",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS arquivo_xml TEXT DEFAULT ''",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS arquivo_pdf TEXT DEFAULT ''",
            ]:
                try:
                    cur.execute(sql)
                except Exception:
                    pass
        conn.commit()


# ── Clientes ──────────────────────────────────────────────────────────────────

def listar_clientes():
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM clientes ORDER BY id")
            return [r[0] for r in cur.fetchall()]


def carregar_cliente(nome: str):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM clientes WHERE id = %s", (nome,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["codigos_nbs"]         = d["codigos_nbs"]         or []
            d["codigos_tributacao"]  = d["codigos_tributacao"]  or []
            return d


def carregar_clientes() -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM clientes ORDER BY id")
            result = {}
            for row in cur.fetchall():
                d = dict(row)
                d["codigos_nbs"]        = d["codigos_nbs"]        or []
                d["codigos_tributacao"] = d["codigos_tributacao"] or []
                result[d["id"]] = d
            return result


def salvar_cliente(nome: str, dados: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clientes
                    (id, caminho_certificado, senha_certificado, cep,
                     lucro_presumido, obra, codigos_nbs, codigos_tributacao,
                     cnpj, razao_social, inscricao_municipal, codigo_ibge)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    caminho_certificado = EXCLUDED.caminho_certificado,
                    senha_certificado   = EXCLUDED.senha_certificado,
                    cep                 = EXCLUDED.cep,
                    lucro_presumido     = EXCLUDED.lucro_presumido,
                    obra                = EXCLUDED.obra,
                    codigos_nbs         = EXCLUDED.codigos_nbs,
                    codigos_tributacao  = EXCLUDED.codigos_tributacao,
                    cnpj                = EXCLUDED.cnpj,
                    razao_social        = EXCLUDED.razao_social,
                    inscricao_municipal = EXCLUDED.inscricao_municipal,
                    codigo_ibge         = EXCLUDED.codigo_ibge
            """, (
                nome,
                dados.get("caminho_certificado", ""),
                dados.get("senha_certificado", ""),
                dados.get("cep", ""),
                dados.get("lucro_presumido", False),
                dados.get("obra", False),
                json.dumps(dados.get("codigos_nbs", [])),
                json.dumps(dados.get("codigos_tributacao", [])),
                dados.get("cnpj", ""),
                dados.get("razao_social", ""),
                dados.get("inscricao_municipal", ""),
                dados.get("codigo_ibge", ""),
            ))
        conn.commit()


def proximo_numero_dps(nome: str) -> int:
    """Retorna o próximo número sequencial de DPS e já incrementa no banco."""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE clientes SET numero_dps = numero_dps + 1
                WHERE id = %s
                RETURNING numero_dps - 1
            """, (nome,))
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else 1


def deletar_cliente(nome: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM clientes WHERE id = %s", (nome,))
        conn.commit()


# ── Tokens ────────────────────────────────────────────────────────────────────

def criar_token(token: str, cliente_id: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tokens (token, cliente_id, ativo) VALUES (%s, %s, TRUE)",
                (token, cliente_id),
            )
        conn.commit()


def excluir_token(token: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tokens WHERE token = %s", (token,))
        conn.commit()


def get_tokens():
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tokens WHERE ativo = TRUE ORDER BY token")
            return _rows(cur)


def get_info_token(token: str):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM tokens WHERE token = %s AND ativo = TRUE", (token,)
            )
            return _row(cur)


# ── Pedidos ───────────────────────────────────────────────────────────────────

def criar_pedido(token: str, cliente_id: str, dados: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pedidos (
                    token, cliente_id,
                    tipo_doc_tomador, inscricao_tomador,
                    sem_cep_tomador,
                    cep_tomador, numero_tomador, complemento_tomador,
                    data_competencia, local_prestacao,
                    descricao_servico, valor_servico,
                    codigo_tributacao, codigo_nbs,
                    retencao_issqn, aliquota_issqn,
                    cep_obra, numero_obra, complemento_obra,
                    status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, 'pendente'
                ) RETURNING id
            """, (
                token, cliente_id,
                dados.get("tipo_doc_tomador", ""),
                dados.get("inscricao_tomador", ""),
                dados.get("sem_cep_tomador", False),
                dados.get("cep_tomador", ""),
                dados.get("numero_tomador", ""),
                dados.get("complemento_tomador", ""),
                dados.get("data_competencia", ""),
                dados.get("local_prestacao", ""),
                dados.get("descricao_servico", ""),
                dados.get("valor_servico", ""),
                dados.get("codigo_tributacao", ""),
                dados.get("codigo_nbs", ""),
                dados.get("retencao_issqn", False),
                dados.get("aliquota_issqn", ""),
                dados.get("cep_obra", ""),
                dados.get("numero_obra", ""),
                dados.get("complemento_obra", ""),
            ))
            pedido_id = cur.fetchone()
            if pedido_id:
                pedido_id = pedido_id[0]
        conn.commit()
    return pedido_id


def get_pedidos(cliente_id: str = None, status: str = None):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            filters, params = [], []
            if cliente_id:
                filters.append("cliente_id = %s")
                params.append(cliente_id)
            if status:
                filters.append("status = %s")
                params.append(status)
            where = ("WHERE " + " AND ".join(filters)) if filters else ""
            cur.execute(f"SELECT * FROM pedidos {where} ORDER BY id DESC", params)
            return _rows(cur)


def get_pedido(pedido_id: int):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pedidos WHERE id = %s", (pedido_id,))
            return _row(cur)


def salvar_arquivos_pedido(pedido_id: int, arquivo_xml: str, arquivo_pdf: str):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pedidos SET arquivo_xml = %s, arquivo_pdf = %s WHERE id = %s",
                (arquivo_xml, arquivo_pdf, pedido_id),
            )
        conn.commit()


def update_status(pedido_id: int, status: str, obs: str = None):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pedidos SET status = %s, observacao = %s WHERE id = %s",
                (status, obs, pedido_id),
            )
        conn.commit()


def excluir_pedido(pedido_id: int):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedidos WHERE id = %s", (pedido_id,))
        conn.commit()


def atualizar_pedido(pedido_id: int, dados: dict):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pedidos SET
                    tipo_doc_tomador    = %s,
                    inscricao_tomador   = %s,
                    sem_cep_tomador     = %s,
                    cep_tomador         = %s,
                    numero_tomador      = %s,
                    complemento_tomador = %s,
                    data_competencia    = %s,
                    local_prestacao     = %s,
                    descricao_servico   = %s,
                    valor_servico       = %s,
                    codigo_tributacao   = %s,
                    codigo_nbs          = %s,
                    retencao_issqn      = %s,
                    aliquota_issqn      = %s,
                    cep_obra            = %s,
                    numero_obra         = %s,
                    complemento_obra    = %s
                WHERE id = %s
            """, (
                dados.get("tipo_doc_tomador", ""),
                dados.get("inscricao_tomador", ""),
                dados.get("sem_cep_tomador", False),
                dados.get("cep_tomador", ""),
                dados.get("numero_tomador", ""),
                dados.get("complemento_tomador", ""),
                dados.get("data_competencia", ""),
                dados.get("local_prestacao", ""),
                dados.get("descricao_servico", ""),
                dados.get("valor_servico", ""),
                dados.get("codigo_tributacao", ""),
                dados.get("codigo_nbs", ""),
                dados.get("retencao_issqn", False),
                dados.get("aliquota_issqn", ""),
                dados.get("cep_obra", ""),
                dados.get("numero_obra", ""),
                dados.get("complemento_obra", ""),
                pedido_id,
            ))
        conn.commit()
