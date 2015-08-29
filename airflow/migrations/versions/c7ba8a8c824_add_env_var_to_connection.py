"""add env_var to connection

Revision ID: c7ba8a8c824
Revises: 338e90f54d61
Create Date: 2015-08-29 12:02:37.905111

"""

# revision identifiers, used by Alembic.
revision = 'c7ba8a8c824'
down_revision = '338e90f54d61'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('connection', sa.Column('env_variable', sa.String(255), nullable=True))


def downgrade():
    op.drop_column('connection', 'env_variable')
