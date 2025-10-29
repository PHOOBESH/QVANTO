from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict
from functools import wraps
import os

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Table, Text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session

# ------------------------
# App & DB setup
# ------------------------
app = Flask(__name__, static_folder=".")
CORS(app)
engine = create_engine("sqlite:///catalog.db", echo=False, future=True)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()
app.url_map.strict_slashes = False 
# ------------------------
# RBAC via simple API keys
# ------------------------
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    api_key = Column(String, unique=True, nullable=False)
    role = Column(String, nullable=False)  # 'viewer' or 'editor'

def current_user():
    key = request.headers.get("X-API-KEY")
    db = SessionLocal()
    try:
        return db.query(User).filter_by(api_key=key).first()
    finally:
        db.close()

def require_role(roles: List[str]):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                return jsonify({"error": "Missing or invalid API key"}), 401
            if user.role not in roles:
                return jsonify({"error": f"Forbidden for role: {user.role}"}), 403
            return fn(*args, **kwargs)
        return wrapper
    return deco

require_view = require_role(["viewer", "editor"])
require_edit = require_role(["editor"])

# ------------------------
# Database Models
# ------------------------
asset_tag_table = Table(
    "asset_tags", Base.metadata,
    Column("asset_id", ForeignKey("assets.id"), primary_key=True),
    Column("tag_id", ForeignKey("tags.id"), primary_key=True),
)

class Asset(Base):
    __tablename__ = "assets"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # policy | claim | reserve_model
    description = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    tags = relationship("Tag", secondary=asset_tag_table, back_populates="assets")

class Tag(Base):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    assets = relationship("Asset", secondary=asset_tag_table, back_populates="tags")

class Lineage(Base):
    __tablename__ = "lineage"
    id = Column(Integer, primary_key=True)
    src_asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    dst_asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    relation = Column(String, default="derives")

class Claim(Base):
    __tablename__ = "claims"
    id = Column(Integer, primary_key=True)
    claim_number = Column(String, unique=True, nullable=False)
    policy_number = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    channel = Column(String, default="agent")
    city = Column(String, default="")
    prior_claims = Column(Integer, default=0)
    fraud_score = Column(Float, default=0.0)
    fraud_label = Column(String, default="OK")
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ------------------------
# Seed Data
# ------------------------
def seed_users():
    db = SessionLocal()
    try:
        if not db.query(User).first():
            db.add_all([
                User(name="Editor", api_key="editor-key-123", role="editor"),
                User(name="Viewer", api_key="viewer-key-123", role="viewer"),
            ])
            db.commit()
    finally:
        db.close()

def seed_demo():
    db = SessionLocal()
    try:
        if not db.query(Tag).first():
            for t in ["PII", "GDPR", "HIPAA", "PCI", "internal", "public"]:
                db.add(Tag(name=t))
            db.commit()
        if not db.query(Asset).first():
            policy = Asset(name="Policy_Master", type="policy", description="Master policy dataset")
            claim = Asset(name="Claim_Intake_2025", type="claim", description="Raw claim intake")
            reserve = Asset(name="Reserve_Model_v1", type="reserve_model", description="Chain-ladder outputs")
            pii = db.query(Tag).filter_by(name="PII").one()
            policy.tags.append(pii)
            db.add_all([policy, claim, reserve])
            db.commit()
            db.add_all([
                Lineage(src_asset_id=policy.id, dst_asset_id=claim.id, relation="feeds"),
                Lineage(src_asset_id=claim.id, dst_asset_id=reserve.id, relation="drives"),
            ])
            db.commit()
        if not db.query(Claim).first():
            db.add_all([
                Claim(claim_number="C-1001", policy_number="P-9001", amount=12000, channel="online", city="Chennai", prior_claims=0, fraud_score=12.0, fraud_label="OK"),
                Claim(claim_number="C-1002", policy_number="P-9002", amount=450000, channel="online", city="Unknown", prior_claims=3, fraud_score=78.0, fraud_label="SUSPICIOUS"),
            ])
            db.commit()
    finally:
        db.close()

seed_users()
seed_demo()

# ------------------------
# Helper
# ------------------------
def asset_to_dict(a: Asset) -> Dict:
    return {
        "id": a.id,
        "name": a.name,
        "type": a.type,
        "description": a.description,
        "created_at": a.created_at.isoformat(),
        "tags": [t.name for t in a.tags],
    }

# ------------------------
# CRUD Endpoints
# ------------------------
@app.get("/assets")
@require_view
def list_assets():
    db = SessionLocal()
    try:
        q = db.query(Asset)
        if typ := request.args.get("type"):
            q = q.filter(Asset.type == typ)
        if tag := request.args.get("tag"):
            q = q.join(Asset.tags).filter(Tag.name == tag)
        return jsonify([asset_to_dict(a) for a in q.all()])
    finally:
        db.close()

@app.post("/assets")
@require_edit
def create_asset():
    data = request.json or {}
    db = SessionLocal()
    try:
        a = Asset(name=data.get("name"), type=data.get("type"), description=data.get("description", ""))
        for t in data.get("tags", []):
            tag_obj = db.query(Tag).filter_by(name=t).first() or Tag(name=t)
            db.add(tag_obj)
            a.tags.append(tag_obj)
        db.add(a)
        db.commit()
        return jsonify(asset_to_dict(a)), 201
    finally:
        db.close()

@app.get("/assets/<int:asset_id>")
@require_view
def get_asset(asset_id):
    db = SessionLocal()
    try:
        a = db.query(Asset).get(asset_id)
        if not a:
            return jsonify({"error": "Not found"}), 404
        return jsonify(asset_to_dict(a))
    finally:
        db.close()

@app.put("/assets/<int:asset_id>")
@require_edit
def update_asset(asset_id):
    data = request.json or {}
    db = SessionLocal()
    try:
        a = db.query(Asset).get(asset_id)
        if not a:
            return jsonify({"error": "Not found"}), 404
        a.name = data.get("name", a.name)
        a.type = data.get("type", a.type)
        a.description = data.get("description", a.description)
        if "tags" in data:
            a.tags.clear()
            for t in data.get("tags", []):
                tag_obj = db.query(Tag).filter_by(name=t).first() or Tag(name=t)
                db.add(tag_obj)
                a.tags.append(tag_obj)
        db.commit()
        return jsonify(asset_to_dict(a))
    finally:
        db.close()

@app.delete("/assets/<int:asset_id>")
@require_edit
def delete_asset(asset_id):
    db = SessionLocal()
    try:
        a = db.query(Asset).get(asset_id)
        if not a:
            return jsonify({"error": "Not found"}), 404
        db.delete(a)
        db.commit()
        return jsonify({"status": "deleted"})
    finally:
        db.close()

# ------------------------
# Tags
# ------------------------
@app.get("/tags")
@require_view
def list_tags():
    db = SessionLocal()
    try:
        return jsonify([t.name for t in db.query(Tag).all()])
    finally:
        db.close()

@app.post("/tags")
@require_edit
def create_tag():
    data = request.json or {}
    db = SessionLocal()
    try:
        name = data.get("name")
        if not name:
            return jsonify({"error": "Missing name"}), 400
        if db.query(Tag).filter_by(name=name).first():
            return jsonify({"error": "Tag exists"}), 400
        t = Tag(name=name)
        db.add(t)
        db.commit()
        return jsonify({"name": t.name}), 201
    finally:
        db.close()

# ------------------------
# Lineage
# ------------------------
@app.route("/lineage", methods=["POST"])

@require_edit
def add_lineage():
    data = request.json or {}
    src_id = data.get("src_asset_id")
    dst_id = data.get("dst_asset_id")

    if not src_id or not dst_id:
        return jsonify({"error": "src_asset_id and dst_asset_id are required and must be integers"}), 400

    db = SessionLocal()
    try:
        src = db.query(Asset).get(src_id)
        dst = db.query(Asset).get(dst_id)
        if not src or not dst:
            return jsonify({"error": "One or both asset IDs do not exist"}), 404

        edge = Lineage(src_asset_id=src_id, dst_asset_id=dst_id,
                       relation=data.get("relation", "derives"))
        db.add(edge)
        db.commit()
        return jsonify({"id": edge.id, "message": "Edge created"}), 201
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.get("/lineage/<int:asset_id>")
@require_view
def get_lineage(asset_id):
    db = SessionLocal()
    try:
        outgoing = db.query(Lineage).filter_by(src_asset_id=asset_id).all()
        incoming = db.query(Lineage).filter_by(dst_asset_id=asset_id).all()
        node_ids = set([asset_id] + [e.dst_asset_id for e in outgoing] + [e.src_asset_id for e in incoming])
        nodes = {a.id: asset_to_dict(a) for a in db.query(Asset).filter(Asset.id.in_(node_ids)).all()}
        edges = [{"id": e.id, "source": e.src_asset_id, "target": e.dst_asset_id, "relation": e.relation} for e in outgoing + incoming]
        return jsonify({"nodes": list(nodes.values()), "edges": edges})
    finally:
        db.close()

# ------------------------
# Fraud Scoring
# ------------------------
RULES = [
    (lambda c: c.amount > 300000, 50.0, "High claim amount > 300k"),
    (lambda c: c.prior_claims >= 3, 25.0, "Three or more prior claims"),
    (lambda c: c.channel == "online", 5.0, "Online submission"),
    (lambda c: c.city.lower() in {"unknown", "na", ""}, 10.0, "Missing/unknown city"),
]
THRESHOLD = 60.0

@app.post("/claims/score")
@require_edit
def score_claim():
    data = request.json or {}
    db = SessionLocal()
    try:
        # Parse safely and convert types
        amount = float(data.get("amount", 0))
        prior_claims = int(data.get("prior_claims", 0))
        channel = (data.get("channel") or "").lower().strip()
        city = (data.get("city") or "").lower().strip()

        # Create temporary claim object for rule evaluation
        claim = Claim(
            claim_number=data.get("claim_number", "NA"),
            policy_number=data.get("policy_number", "NA"),
            amount=amount,
            channel=channel,
            city=city,
            prior_claims=prior_claims,
        )

        # Apply rules
        score = 0.0
        reasons = []
        for rule, weight, note in RULES:
            try:
                if rule(claim):
                    score += weight
                    reasons.append(note)
            except Exception:
                pass

        # Assign results
        claim.fraud_score = min(score, 100.0)
        claim.fraud_label = "SUSPICIOUS" if claim.fraud_score >= THRESHOLD else "OK"

        # Store in DB
        db.add(claim)
        db.commit()

        return jsonify({
            "claim_number": claim.claim_number,
            "fraud_label": claim.fraud_label,
            "fraud_score": claim.fraud_score,
            "reasons": reasons,
        }), 200
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# ------------------------
# Homepage Route (Fix for 404)
# ------------------------
@app.route("/")
def home():
    return send_from_directory(os.path.dirname(__file__), "index.html")

@app.get("/health")
def health():
    return {"ok": True}

if __name__ == "__main__":
    app.run(debug=True)
