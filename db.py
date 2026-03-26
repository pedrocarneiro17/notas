"""
Camada de banco de dados unificada — PostgreSQL (Railway).
Substitui clientes.py (JSON) e webapp_db.py (SQLite).
"""
import os
import json
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


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
                    codigos_tributacao  JSONB DEFAULT '[]'
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
                     lucro_presumido, obra, codigos_nbs, codigos_tributacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    caminho_certificado = EXCLUDED.caminho_certificado,
                    senha_certificado   = EXCLUDED.senha_certificado,
                    cep                 = EXCLUDED.cep,
                    lucro_presumido     = EXCLUDED.lucro_presumido,
                    obra                = EXCLUDED.obra,
                    codigos_nbs         = EXCLUDED.codigos_nbs,
                    codigos_tributacao  = EXCLUDED.codigos_tributacao
            """, (
                nome,
                dados.get("caminho_certificado", ""),
                dados.get("senha_certificado", ""),
                dados.get("cep", ""),
                dados.get("lucro_presumido", False),
                dados.get("obra", False),
                json.dumps(dados.get("codigos_nbs", [])),
                json.dumps(dados.get("codigos_tributacao", [])),
            ))
        conn.commit()


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
                )
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
        conn.commit()


def get_pedidos(cliente_id: str = None):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if cliente_id:
                cur.execute(
                    "SELECT * FROM pedidos WHERE cliente_id = %s ORDER BY id DESC",
                    (cliente_id,)
                )
            else:
                cur.execute("SELECT * FROM pedidos ORDER BY id DESC")
            return _rows(cur)


def get_pedido(pedido_id: int):
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pedidos WHERE id = %s", (pedido_id,))
            return _row(cur)


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
