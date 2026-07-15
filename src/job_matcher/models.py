from __future__ import annotations

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

from pgvector.sqlalchemy import Vector

Base = declarative_base()


class JobOffer(Base):
    __tablename__ = "job_offers"

    id = Column(Integer, primary_key=True)
    external_job_id = Column(Text, index=True)
    canonical_url = Column(Text, unique=True, nullable=False)
    source = Column(Text, nullable=True, index=True)
    source_url = Column(Text, nullable=True)
    search_url = Column(Text, nullable=True)
    title = Column(Text, nullable=True)
    title_embedding = Column(Vector(384), nullable=True)
    company = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    date_posted = Column(DateTime(timezone=True), nullable=True, index=True)
    valid_through = Column(DateTime(timezone=True), nullable=True)
    employment_type = Column(Text, nullable=True)
    industry = Column(Text, nullable=True)
    skills = Column(Text, nullable=True)
    education_requirements = Column(Text, nullable=True)
    address_country = Column(Text, nullable=True)
    address_locality = Column(Text, nullable=True)
    address_region = Column(Text, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    description_text = Column(Text, nullable=True)
    description_html = Column(Text, nullable=True)
    criteria_json = Column(Text, nullable=True)
    source_parser = Column(Text, nullable=True)
    detail_status = Column(Text, nullable=True)
    detail_error = Column(Text, nullable=True)
    collected_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    paragraphs = relationship(
        "JobParagraph",
        back_populates="job_offer",
        cascade="all, delete-orphan",
    )


class JobParagraph(Base):
    __tablename__ = "job_paragraphs"
    __table_args__ = (
        UniqueConstraint("job_offer_id", "paragraph_idx", name="uq_job_paragraph_idx"),
        Index(
            "ix_job_paragraphs_embedding",
            "embedding",
            postgresql_using="ivfflat",
            postgresql_with={"lists": 100},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )

    id = Column(Integer, primary_key=True)
    job_offer_id = Column(Integer, ForeignKey("job_offers.id", ondelete="CASCADE"))
    paragraph_idx = Column(Integer, nullable=False)
    paragraph = Column(Text, nullable=False)
    paragraph_chars = Column(Integer, nullable=False)
    embedding = Column(Vector(384), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    job_offer = relationship("JobOffer", back_populates="paragraphs")
