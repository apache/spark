"""task_duration

Revision ID: 2e541a1dcfed
Revises: 1b38cef5b76e
Create Date: 2015-10-28 20:38:41.266143

"""

# revision identifiers, used by Alembic.
revision = '2e541a1dcfed'
down_revision = '1b38cef5b76e'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    op.alter_column('task_instance', 'duration',
               existing_type=mysql.INTEGER(display_width=11),
               type_=sa.Float(),
               existing_nullable=True)


def downgrade():
    pass
