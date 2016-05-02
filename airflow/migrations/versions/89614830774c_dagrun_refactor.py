"""DagRun refactor

Revision ID: 89614830774c
Revises: 2e82aab8ef20
Create Date: 2016-04-16 18:50:44.195014

"""

# revision identifiers, used by Alembic.
revision = '89614830774c'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None

from alembic import op
from alembic import context
import sqlalchemy as sa
from sqlalchemy.sql.expression import false


def upgrade():
    url = context.config.get_main_option("sqlalchemy.url")
    if url.find("postgresql") > -1:
        op.drop_constraint("dag_run_dag_id_execution_date_key", "dag_run")
        # op.create_unique_constraint("uq_dag_run_dag_id_execution_date_run_id",
        #                             "dag_run", ["dag_id", "execution_date", "run_id"])
    elif url.find("mysql") > -1:
        op.drop_constraint("dag_id_2", "dag_run", "unique")
        # op.create_unique_constraint("uq_dag_run_dag_id_execution_date_run_id",
        #                             "dag_run", ["dag_id", "execution_date"])

    with op.batch_alter_table('dag_run') as batch_op:
        batch_op.drop_column('id')
        batch_op.drop_column('external_trigger')
        batch_op.drop_column('run_id')
        batch_op.alter_column(
            'state',
            existing_type=sa.String(50),
            existing_nullable=False)
        batch_op.add_column(sa.Column('lock_id', sa.Integer))
        batch_op.create_primary_key('pk_dag_run', ['dag_id', 'execution_date'])


    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column(
            column_name='last_scheduler_run',
            new_column_name='last_scheduled',
            existing_type=sa.DateTime,
            existing_nullable=True)

def downgrade():
    with op.batch_alter_table('dag_run') as batch_op:
        batch_op.drop_column('lock_id')
        batch_op.add_column(sa.Column('external_trigger', sa.Boolean))
        batch_op.add_column(sa.Column('run_id', sa.String))
        batch_op.drop_constraint('pk_dag_run', type_='primary')
        batch_op.create_unique_constraint('unique_run_id', ['dag_id', 'run_id'])
        batch_op.add_column(sa.Column('id', sa.Integer, primary_key=True))

    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column(
            column_name='last_scheduled',
            new_column_name='last_scheduler_run',
            existing_type=sa.DateTime,
            existing_nullable=True)
