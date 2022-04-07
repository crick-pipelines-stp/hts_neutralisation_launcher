import sqlalchemy as sql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime


# so we can do utc_now() timestamps with sqlalchemy
class utcnow(expression.FunctionElement):
    type = DateTime()


@compiles(utcnow, "mysql")
def utc_now(element, compiler, **kw):
    return "UTC_TIMESTAMP()"


Base = declarative_base()


class Analysis(Base):
    __tablename__ = "NE_task_tracking_analysis"
    id = sql.Column(sql.Integer, primary_key=True)
    workflow_id = sql.Column(sql.Integer, nullable=False)
    variant = sql.Column(sql.String(45), nullable=False)
    created_at = sql.Column(sql.TIMESTAMP, default=utcnow(), nullable=False)
    finished_at = sql.Column(sql.TIMESTAMP)


class Stitching(Base):
    __tablename__ = "NE_task_tracking_stitching"
    id = sql.Column(sql.Integer, primary_key=True)
    plate_name = sql.Column(sql.String(45), nullable=False)
    created_at = sql.Column(sql.TIMESTAMP, server_default=utcnow(), nullable=False)
    finished_at = sql.Column(sql.TIMESTAMP)


class Variant(Base):
    __tablename__ = "NE_available_strains"
    id = sql.Column(sql.Integer, primary_key=True)
    mutant_strain = sql.Column(sql.String(45))
    plate_id_1 = sql.Column(sql.String(5))
    plate_id_2 = sql.Column(sql.String(5))


class Titration(Base):
    __tablename__ = "NE_task_tracking_analysis_titration"
    id = sql.Column(sql.Integer, primary_key=True)
    workflow_id = sql.Column(sql.Integer, nullable=False)
    variant = sql.Column(sql.String(45), nullable=False)
    created_at = sql.Column(sql.TIMESTAMP, default=utcnow(), nullable=False)
    finished_at = sql.Column(sql.TIMESTAMP)
