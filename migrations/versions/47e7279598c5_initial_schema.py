"""initial schema with timescale support

Revision ID: 47e7279598c5
Revises: 
Create Date: 2026-04-21 12:11:09.122865

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers
revision = '47e7279598c5'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()

    # =========================================
    # EXTENSIONS (Timescale)
    # =========================================
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

    # =========================================
    # ENUMS
    # =========================================
    status_type = sa.Enum('inserted', 'authorized', 'non_active', name='status_type')
    user_role = sa.Enum('admin', 'staff', name='user_role')

    status_type.create(bind, checkfirst=True)
    user_role.create(bind, checkfirst=True)

    # =========================================
    # care_providers
    # =========================================
    op.create_table(
        'care_providers',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('full_name', sa.String(255), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text('now()'))
    )

    # =========================================
    # users
    # =========================================
    op.create_table(
        'users',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('username', sa.String, nullable=False),
        sa.Column('password_hash', sa.Text, nullable=False),
        sa.Column('full_name', sa.String),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True),
                  server_default=sa.text('now()')),
        sa.Column('last_login', sa.TIMESTAMP(timezone=True)),
        sa.Column('is_active', sa.Boolean, nullable=False,
                  server_default=sa.text('true')),
        sa.Column('role', postgresql.ENUM('admin', 'staff', name='user_role',
                  create_type=False), nullable=False,
                  server_default=sa.text("'staff'::user_role")),
        sa.UniqueConstraint('username')
    )

    # =========================================
    # staff_profiles
    # =========================================
    op.create_table(
        'staff_profiles',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('users.id', ondelete='CASCADE'),
                  nullable=False, unique=True),
        sa.Column('care_provider_id', sa.Integer,
                  sa.ForeignKey('care_providers.id'), nullable=False)
    )

    # =========================================
    # devices
    # =========================================
    op.create_table(
        'devices',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('care_provider_id', sa.Integer,
                  sa.ForeignKey('care_providers.id'), nullable=False),
        sa.Column('email_address', sa.String, nullable=False),
        sa.Column('authorization_status',
                  postgresql.ENUM('inserted', 'authorized', 'non_active',
                  name='status_type', create_type=False),
                  nullable=False, server_default=sa.text("'inserted'::status_type")),
        sa.Column('device_type', sa.String(50)),
        sa.Column('daily_summaries_checkpoint', sa.Date),
        sa.Column('sleep_checkpoint', sa.Date),
        sa.Column('intraday_checkpoint', sa.TIMESTAMP(timezone=True)),
        sa.Column('last_synch', sa.TIMESTAMP(timezone=True)),
        sa.Column('access_token', sa.Text),
        sa.Column('refresh_token', sa.Text),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True),
                  server_default=sa.text('now()'))
    )

    # =========================================
    # pending_authorizations
    # =========================================
    op.create_table(
        'pending_authorizations',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('device_id', sa.Integer, sa.ForeignKey('devices.id'),
                  nullable=False),
        sa.Column('state', sa.String(500), nullable=False, unique=True),
        sa.Column('code_verifier', sa.String(128), nullable=False),
        sa.Column('expires_at', sa.TIMESTAMP, nullable=False),
        sa.Column('created_at', sa.TIMESTAMP)
    )
    op.create_index('idx_pending_auth_state', 'pending_authorizations', ['state'])
    op.create_index('idx_pending_auth_expires', 'pending_authorizations', ['expires_at'])

    # =========================================
    # daily_summaries
    # Note: uses (device_id, date) composite PK — not a hypertable
    # (TimescaleDB requires a timestamp column as time dimension)
    # =========================================
    op.create_table(
        'daily_summaries',
        sa.Column('device_id', sa.Integer, sa.ForeignKey('devices.id'),
                  nullable=False),
        sa.Column('date', sa.Date, nullable=False),
        sa.Column('steps', sa.Integer),
        sa.Column('heart_rate', sa.Integer),
        sa.Column('sleep_minutes', sa.Integer),
        sa.Column('calories', sa.Integer),
        sa.Column('distance', sa.Float),
        sa.Column('floors', sa.Integer),
        sa.Column('elevation', sa.Float),
        sa.Column('active_minutes', sa.Integer),
        sa.Column('sedentary_minutes', sa.Integer),
        sa.Column('nutrition_calories', sa.Integer),
        sa.Column('water', sa.Float),
        sa.Column('weight', sa.Float),
        sa.Column('bmi', sa.Float),
        sa.Column('fat', sa.Float),
        sa.Column('oxygen_saturation', sa.Float),
        sa.Column('respiratory_rate', sa.Float),
        sa.Column('temperature', sa.Float),
        sa.UniqueConstraint('device_id', 'date', name='daily_summaries_email_id_date_key')
    )
    op.create_index('daily_summaries_date_idx', 'daily_summaries',
                    [sa.literal_column('date DESC')])

    # =========================================
    # intraday_metrics (hypertable)
    # =========================================
    op.create_table(
        'intraday_metrics',
        sa.Column('device_id', sa.Integer, sa.ForeignKey('devices.id'),
                  nullable=False),
        sa.Column('time', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('heart_rate', sa.Float),
        sa.Column('steps', sa.Float),
        sa.Column('calories', sa.Float),
        sa.Column('distance', sa.Float),
        sa.Column('floors', sa.Integer),
        sa.Column('elevation', sa.Integer),
        sa.PrimaryKeyConstraint('device_id', 'time')
    )
    op.create_index('intraday_metrics_time_idx', 'intraday_metrics',
                    [sa.literal_column('time DESC')])
    op.execute("""
        SELECT create_hypertable(
            'intraday_metrics',
            'time',
            if_not_exists => TRUE
        );
    """)

    # =========================================
    # sleep_sessions
    # =========================================
    op.create_table(
        'sleep_sessions',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('device_id', sa.Integer, sa.ForeignKey('devices.id'),
                  nullable=False)
    )

    # =========================================
    # sleep_logs (hypertable)
    # =========================================
    op.create_table(
        'sleep_logs',
        sa.Column('sleep_session_id', sa.Integer,
                  sa.ForeignKey('sleep_sessions.id'), nullable=False),
        sa.Column('start_time', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('end_time', sa.TIMESTAMP(timezone=True)),
        sa.Column('is_main_sleep', sa.Boolean),
        sa.Column('duration', sa.Integer),
        sa.Column('minutes_asleep', sa.Integer),
        sa.Column('minutes_awake', sa.Integer),
        sa.Column('minutes_in_the_bed', sa.Integer),
        sa.Column('log_type', sa.String(50)),
        sa.Column('type', sa.String(50)),
        sa.PrimaryKeyConstraint('sleep_session_id', 'start_time')
    )
    op.create_index('sleep_logs_start_time_idx', 'sleep_logs',
                    [sa.literal_column('start_time DESC')])
    op.execute("""
        SELECT create_hypertable(
            'sleep_logs',
            'start_time',
            if_not_exists => TRUE
        );
    """)

    # =========================================
    # sleep_levels (hypertable)
    # =========================================
    op.create_table(
        'sleep_levels',
        sa.Column('sleep_session_id', sa.Integer,
                  sa.ForeignKey('sleep_sessions.id'), nullable=False),
        sa.Column('time', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('level', sa.String(50)),
        sa.Column('seconds', sa.Integer),
        sa.PrimaryKeyConstraint('sleep_session_id', 'time')
    )
    op.create_index('sleep_levels_time_idx', 'sleep_levels',
                    [sa.literal_column('time DESC')])
    op.execute("""
        SELECT create_hypertable(
            'sleep_levels',
            'time',
            if_not_exists => TRUE
        );
    """)

    # =========================================
    # sleep_short_levels (hypertable)
    # =========================================
    op.create_table(
        'sleep_short_levels',
        sa.Column('sleep_session_id', sa.Integer,
                  sa.ForeignKey('sleep_sessions.id'), nullable=False),
        sa.Column('time', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('seconds', sa.Integer),
        sa.PrimaryKeyConstraint('sleep_session_id', 'time')
    )
    op.create_index('sleep_short_levels_time_idx', 'sleep_short_levels',
                    [sa.literal_column('time DESC')])
    op.execute("""
        SELECT create_hypertable(
            'sleep_short_levels',
            'time',
            if_not_exists => TRUE
        );
    """)


def downgrade():
    op.drop_table('staff_profiles')
    op.drop_table('pending_authorizations')
    op.drop_table('sleep_short_levels')
    op.drop_table('sleep_levels')
    op.drop_table('sleep_logs')
    op.drop_table('sleep_sessions')
    op.drop_table('intraday_metrics')
    op.drop_table('daily_summaries')
    op.drop_table('devices')
    op.drop_table('users')
    op.drop_table('care_providers')

    sa.Enum(name='status_type').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='user_role').drop(op.get_bind(), checkfirst=True)
    # ### end Alembic commands ###
