import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pedidos.db")


class Database:
    def _conn(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token      TEXT PRIMARY KEY,
                    cliente_id TEXT NOT NULL,
                    ativo      INTEGER DEFAULT 1,
                    criado_em  TEXT
                );
                CREATE TABLE IF NOT EXISTS pedidos (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    token               TEXT,
                    cliente_id          TEXT,
                    tipo_doc_tomador    TEXT,
                    inscricao_tomador   TEXT,
                    cep_tomador         TEXT,
                    numero_tomador      TEXT,
                    complemento_tomador TEXT,
                    data_competencia    TEXT,
                    local_prestacao     TEXT,
                    descricao_servico   TEXT,
                    valor_servico       TEXT,
                    codigo_nbs          TEXT,
                    codigo_tributacao   TEXT,
                    retencao_issqn      INTEGER DEFAULT 0,
                    aliquota_issqn      TEXT,
                    sem_cep_tomador     INTEGER DEFAULT 0,
                    cep_obra            TEXT,
                    numero_obra         TEXT,
                    complemento_obra    TEXT,
                    status              TEXT DEFAULT 'pendente',
                    criado_em           TEXT,
                    observacao          TEXT
                );
            """)
        # Migração: adiciona colunas novas em bancos existentes
        migracoes = [
            "ALTER TABLE pedidos ADD COLUMN sem_cep_tomador  INTEGER DEFAULT 0",
            "ALTER TABLE pedidos ADD COLUMN cep_obra         TEXT",
            "ALTER TABLE pedidos ADD COLUMN numero_obra      TEXT",
            "ALTER TABLE pedidos ADD COLUMN complemento_obra TEXT",
        ]
        with self._conn() as conn:
            for sql in migracoes:
                try:
                    conn.execute(sql)
                except Exception:
                    pass  # coluna já existe

    # ── Tokens ────────────────────────────────────────────────

    def criar_token(self, token: str, cliente_id: str):
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO tokens VALUES (?, ?, 1, ?)",
                (token, cliente_id, datetime.now().isoformat()),
            )

    def excluir_token(self, token: str):
        with self._conn() as conn:
            conn.execute("DELETE FROM tokens WHERE token=?", (token,))

    def get_tokens(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM tokens WHERE ativo=1 ORDER BY criado_em DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_info_token(self, token: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM tokens WHERE token=? AND ativo=1", (token,)
            ).fetchone()
            return dict(row) if row else None

    # ── Pedidos ───────────────────────────────────────────────

    def criar_pedido(self, token: str, cliente_id: str, dados: dict):
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO pedidos (
                    token, cliente_id,
                    tipo_doc_tomador, inscricao_tomador,
                    cep_tomador, numero_tomador, complemento_tomador,
                    data_competencia, local_prestacao,
                    descricao_servico, valor_servico,
                    codigo_nbs, codigo_tributacao,
                    retencao_issqn, aliquota_issqn,
                    status, criado_em
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendente', ?)
                """,
                (
                    token, cliente_id,
                    dados["tipo_doc_tomador"], dados["inscricao_tomador"],
                    dados["cep_tomador"], dados["numero_tomador"],
                    dados["complemento_tomador"],
                    dados["data_competencia"], dados["local_prestacao"],
                    dados["descricao_servico"], dados["valor_servico"],
                    dados["codigo_nbs"], dados["codigo_tributacao"],
                    1 if dados["retencao_issqn"] else 0,
                    dados.get("aliquota_issqn", ""),
                    datetime.now().isoformat(),
                ),
            )

    def get_pedidos(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pedidos ORDER BY criado_em DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_pedido(self, pedido_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pedidos WHERE id=?", (pedido_id,)
            ).fetchone()
            return dict(row) if row else None

    def update_status(self, pedido_id: int, status: str, obs: str = None):
        with self._conn() as conn:
            conn.execute(
                "UPDATE pedidos SET status=?, observacao=? WHERE id=?",
                (status, obs, pedido_id),
            )

    def excluir_pedido(self, pedido_id: int):
        with self._conn() as conn:
            conn.execute("DELETE FROM pedidos WHERE id=?", (pedido_id,))

    def get_pedidos_por_cliente(self, cliente_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pedidos WHERE cliente_id=? ORDER BY criado_em DESC",
                (cliente_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def atualizar_pedido(self, pedido_id: int, dados: dict):
        with self._conn() as conn:
            conn.execute(
                """UPDATE pedidos SET
                    tipo_doc_tomador=?, inscricao_tomador=?,
                    cep_tomador=?, numero_tomador=?, complemento_tomador=?,
                    data_competencia=?, local_prestacao=?,
                    descricao_servico=?, valor_servico=?,
                    codigo_nbs=?, codigo_tributacao=?,
                    retencao_issqn=?, aliquota_issqn=?
                WHERE id=?""",
                (
                    dados["tipo_doc_tomador"], dados["inscricao_tomador"],
                    dados["cep_tomador"], dados["numero_tomador"],
                    dados["complemento_tomador"], dados["data_competencia"],
                    dados["local_prestacao"], dados["descricao_servico"],
                    dados["valor_servico"], dados["codigo_nbs"],
                    dados["codigo_tributacao"],
                    1 if dados["retencao_issqn"] else 0,
                    dados.get("aliquota_issqn", ""),
                    pedido_id,
                ),
            )
