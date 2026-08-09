"""Add pgvector candidate recall for Cloud Memory.

Revision ID: 20260809_07
Revises: 20260809_06
"""

from alembic import op


revision = "20260809_07"
down_revision = "20260809_06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("ALTER TABLE memory_items ADD COLUMN embedding_vector vector(1024)")
    op.execute(
        "UPDATE memory_items "
        "SET embedding_vector = embedding::text::vector(1024) "
        "WHERE embedding IS NOT NULL AND json_array_length(embedding) = 1024"
    )
    op.execute(
        "CREATE INDEX ix_memory_items_embedding_hnsw "
        "ON memory_items USING hnsw (embedding_vector vector_cosine_ops) "
        "WHERE status = 'active' AND embedding_vector IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_memory_items_embedding_hnsw")
    op.execute("ALTER TABLE memory_items DROP COLUMN embedding_vector")
