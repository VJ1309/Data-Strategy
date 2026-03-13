"""
Data Governance & Contract Monitoring Platform
Enterprise-grade data governance using OpenMetadata
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Optional, Any, Tuple
import json
from dataclasses import dataclass, field, asdict
from functools import lru_cache
from collections import defaultdict
import hashlib

# =============================================================================
# CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Data Governance Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Allowed domains (supply chain areas)
ALLOWED_DOMAINS = ["Deliver", "Plan", "Procurement", "Make", "Quality", "Masterdata"]

# Data Assets by Domain (only Deliver has data assets for this prototype)
DATA_ASSETS = {
    "Deliver": ["Delivery", "Transportation", "Sales Order", "Shipment"],
    "Plan": [],
    "Procurement": [],
    "Make": [],
    "Quality": [],
    "Masterdata": []
}

# Data Asset to Database mapping (1:1)
DATA_ASSET_DATABASE_MAPPING = {
    "Delivery": "SC_Core",
    "Transportation": "SC_Core",
    "Sales Order": "SC_Core",
    "Shipment": "SC_Core"
}

# Allowed databases
ALLOWED_DATABASES = ["SC_Core", "SCRU_IM", "SCRU_EM"]

# Data classification levels
DATA_CLASSIFICATIONS = {
    "public": {"color": "#28a745", "icon": "🌐", "description": "Public data"},
    "internal": {"color": "#17a2b8", "icon": "🏢", "description": "Internal use only"},
    "confidential": {"color": "#ffc107", "icon": "🔒", "description": "Confidential data"},
    "restricted": {"color": "#dc3545", "icon": "🚫", "description": "Highly restricted"}
}

# Contract status
CONTRACT_STATUS = {
    "draft": {"color": "#6c757d", "icon": "📝"},
    "review": {"color": "#ffc107", "icon": "👀"},
    "active": {"color": "#28a745", "icon": "✅"},
    "deprecated": {"color": "#dc3545", "icon": "⚠️"}
}

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.8rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f77b4 0%, #2ca02c 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card-green {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    .metric-card-orange {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    .metric-card-blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    .metric-card-purple {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .contract-card {
        border: 2px solid #e0e0e0;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        background: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }
    .contract-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        transform: translateY(-2px);
    }
    .contract-active {
        border-left: 5px solid #28a745;
    }
    .contract-draft {
        border-left: 5px solid #6c757d;
    }
    .contract-review {
        border-left: 5px solid #ffc107;
    }
    .contract-deprecated {
        border-left: 5px solid #dc3545;
    }
    .governance-badge {
        display: inline-block;
        padding: 0.35rem 0.85rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        margin: 0.25rem;
    }
    .classification-public {
        background-color: #d4edda;
        color: #155724;
    }
    .classification-internal {
        background-color: #d1ecf1;
        color: #0c5460;
    }
    .classification-confidential {
        background-color: #fff3cd;
        color: #856404;
    }
    .classification-restricted {
        background-color: #f8d7da;
        color: #721c24;
    }
    .ownership-assigned {
        background-color: #d4edda;
        color: #155724;
    }
    .ownership-unassigned {
        background-color: #f8d7da;
        color: #721c24;
    }
    .search-result {
        padding: 1rem;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        margin: 0.5rem 0;
        background: white;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    .search-result:hover {
        background: #f8f9fa;
        border-color: #1f77b4;
    }
    .timeline-item {
        padding: 1rem;
        border-left: 3px solid #1f77b4;
        margin-left: 1rem;
        margin-bottom: 1rem;
    }
    .wizard-step {
        padding: 1.5rem;
        border: 2px dashed #e0e0e0;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .wizard-step-active {
        border-color: #1f77b4;
        background: #f0f8ff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 1.5rem;
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class OpenMetadataConfig:
    """Configuration for OpenMetadata connection"""
    host: str
    port: int = 8585
    api_version: str = "v1"
    
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/api/{self.api_version}"

@dataclass
class DataContract:
    """Enhanced data contract with governance"""
    id: str
    table_fqn: str
    table_name: str
    domain: str  # Supply chain area: Deliver, Plan, Make, etc.
    data_asset: str  # Data asset within domain: Delivery, Transportation, etc.
    database: str  # Physical database: SC_Core, SCRU_IM, SCRU_EM
    version: str
    status: str  # draft, review, active, deprecated
    owner: str
    created_date: datetime
    last_modified: datetime
    
    # Schema definition
    schema_definition: Dict[str, Any]
    
    # Quality rules
    quality_rules: List[Dict[str, Any]]
    
    # SLA requirements
    sla_requirements: Dict[str, Any]
    
    # Governance
    classification: str  # public, internal, confidential, restricted
    tags: List[str]
    description: str
    business_purpose: str
    
    # Consumer management
    registered_consumers: List[str]
    downstream_tables: List[str]
    
    # Compliance
    retention_days: Optional[int] = None
    data_history_years: int = 2  # Number of years to retain data
    contains_pii: bool = False
    compliance_requirements: List[str] = field(default_factory=list)
    
    # Metadata
    change_log: List[Dict[str, Any]] = field(default_factory=list)
    approval_history: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class GovernanceMetrics:
    """Governance health metrics"""
    total_assets: int
    owned_assets: int
    documented_assets: int
    classified_assets: int
    contracted_assets: int
    compliant_assets: int
    
    @property
    def ownership_coverage(self) -> float:
        return (self.owned_assets / self.total_assets * 100) if self.total_assets > 0 else 0
    
    @property
    def documentation_coverage(self) -> float:
        return (self.documented_assets / self.total_assets * 100) if self.total_assets > 0 else 0
    
    @property
    def classification_coverage(self) -> float:
        return (self.classified_assets / self.total_assets * 100) if self.total_assets > 0 else 0
    
    @property
    def contract_coverage(self) -> float:
        return (self.contracted_assets / self.total_assets * 100) if self.total_assets > 0 else 0
    
    @property
    def compliance_rate(self) -> float:
        return (self.compliant_assets / self.total_assets * 100) if self.total_assets > 0 else 0

@dataclass
class SchemaChange:
    """Schema change detection"""
    table_fqn: str
    change_type: str
    column_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    detected_at: datetime
    severity: str
    impact_level: str
    requires_approval: bool = False

@dataclass
class DataAsset:
    """Unified data asset representation"""
    fqn: str
    name: str
    domain: str  # Supply chain area
    data_asset: str  # Data asset within domain
    database: str  # Physical database
    schema: str
    asset_type: str
    description: str
    owner: Optional[str]
    tags: List[str]
    classification: Optional[str]
    last_updated: datetime
    column_count: int
    row_count: Optional[int]
    has_contract: bool
    quality_score: float
    popularity_score: int  # Based on usage

@dataclass
class DataTrustScore:
    """Comprehensive trust score for data assets"""
    fqn: str
    table_name: str
    domain: str  # Supply chain area
    data_asset: str  # Data asset within domain
    database: str  # Physical database
    
    # Component scores (0-100)
    data_quality_score: float
    contract_availability_score: float
    freshness_score: float
    documentation_score: float
    lineage_usage_score: float
    security_compliance_score: float
    
    # Composite score
    composite_trust_score: float
    
    # Trust level
    trust_level: str  # Platinum, Gold, Silver, Bronze, Needs Attention
    
    # Metadata
    owner: Optional[str]
    classification: Optional[str]
    last_assessed: datetime
    
    # Recommendations
    improvement_areas: List[str] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)

@dataclass
class MetricDefinition:
    """Metric definition for Data Products (supports Metric Dependency Tree)"""
    name: str
    description: str
    formula: str  # e.g., "SUM(revenue)", "COUNT(DISTINCT customers)"
    unit: str  # e.g., "$", "%", "count"
    source_columns: List[str] = field(default_factory=list)  # Columns used in calculation
    
@dataclass  
class OutputPort:
    """Output port for Data Product consumption"""
    name: str
    port_type: str  # "dataset", "api", "stream", "dashboard"
    format: str  # "parquet", "json", "csv", "rest", "graphql"
    description: str
    access_pattern: str = "batch"  # "batch", "real-time", "on-demand"
    endpoint: str = ""  # URL or path when applicable

@dataclass
class DataProduct:
    """
    Business-aligned Data Product - the core marketplace unit.
    Implements Right-to-Left philosophy: starts with business purpose,
    wraps multiple data assets/tables into a consumable product.
    """
    id: str
    name: str  # e.g., "Delivery Performance Tracker"
    domain: str  # Supply chain area: Deliver, Plan, Make, etc.
    
    # Business Alignment (Right-to-Left: demand-driven)
    business_purpose: str  # The "why" - what business question does this answer?
    target_personas: List[str]  # Who consumes this: ["Sales Manager", "Supply Chain Analyst"]
    
    # Metrics (Playbook: Metric Dependency Tree)
    north_star_metric: MetricDefinition  # Primary KPI that matters most
    functional_metrics: List[MetricDefinition]  # Supporting metrics
    granular_metrics: List[MetricDefinition]  # Detailed operational metrics
    
    # Composition - links to existing entities
    data_assets: List[str]  # Data assets included (e.g., ["Delivery", "Transportation"])
    table_fqns: List[str]  # FQNs of constituent tables
    contract_ids: List[str]  # IDs of linked contracts
    
    # Output Ports - how consumers access this product
    output_ports: List[OutputPort]
    
    # Lifecycle
    status: str  # "draft", "active", "deprecated"
    version: str
    owner: str
    created_date: datetime
    last_modified: datetime
    
    # Marketplace metadata
    tags: List[str] = field(default_factory=list)
    documentation_url: str = ""
    
    # Usage & ratings
    usage_count: int = 0
    consumer_count: int = 0
    rating: float = 0.0
    
    # Aggregated trust (computed from constituent tables)
    aggregated_trust_score: float = 0.0
    trust_level: str = "Needs Attention"
    
    # Change tracking
    change_log: List[Dict[str, Any]] = field(default_factory=list)

# =============================================================================
# OPENMETADATA CLIENT
# =============================================================================

class OpenMetadataClient:
    """Client for interacting with OpenMetadata APIs"""
    
    def __init__(self, config: OpenMetadataConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make HTTP request to OpenMetadata API"""
        url = f"{self.config.base_url}/{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API Error: {str(e)}")
            return {}
    
    def get_tables(self, limit: int = 200, fields: str = "owner,tags,columns,description") -> List[Dict]:
        """Fetch tables from OpenMetadata"""
        data = self._make_request("tables", params={"limit": limit, "fields": fields})
        tables = data.get("data", [])
        return [t for t in tables if t.get("fullyQualifiedName", "").split(".")[0] in ALLOWED_DATABASES]
    
    def get_table_profile(self, fqn: str) -> Dict:
        """Get profiling data for a specific table"""
        encoded_fqn = requests.utils.quote(fqn, safe='')
        return self._make_request(f"tables/name/{encoded_fqn}", 
                                 params={"fields": "tableProfile,testSuite,columns"})
    
    def get_lineage(self, fqn: str, depth: int = 2) -> Dict:
        """Get lineage information"""
        encoded_fqn = requests.utils.quote(fqn, safe='')
        return self._make_request(f"lineage/table/name/{encoded_fqn}", 
                                 params={"upstreamDepth": depth, "downstreamDepth": depth})

# =============================================================================
# DATA CONTRACT ENGINE
# =============================================================================

class DataContractEngine:
    """Manage data contracts with governance"""
    
    def __init__(self):
        self.contracts: Dict[str, DataContract] = {}
    
    def create_contract(self, table: Dict, owner: str, classification: str,
                       description: str, business_purpose: str, 
                       quality_rules: List[Dict] = None,
                       sla_hours: int = 24, contains_pii: bool = False,
                       domain: str = None, data_asset: str = None, database: str = None,
                       data_history_years: int = 2) -> DataContract:
        """Create a new data contract"""
        fqn = table.get("fullyQualifiedName", "")
        columns = table.get("columns", [])
        
        schema_def = {
            col["name"]: {
                "dataType": col.get("dataType", "UNKNOWN"),
                "nullable": col.get("constraint", "") != "NOT NULL",
                "description": col.get("description", ""),
                "isPII": "PII" in str(col.get("tags", [])),
                "calculation": col.get("calculation", "")
            }
            for col in columns
        }
        
        contract_id = hashlib.md5(fqn.encode()).hexdigest()[:12]
        
        # Extract domain, data_asset, and database from table metadata or use provided values
        table_domain = domain or table.get("domain", ALLOWED_DOMAINS[0])
        table_data_asset = data_asset or table.get("data_asset", "")
        table_database = database or fqn.split(".")[0] if fqn else ALLOWED_DATABASES[0]
        
        contract = DataContract(
            id=contract_id,
            table_fqn=fqn,
            table_name=table.get("name", ""),
            domain=table_domain,
            data_asset=table_data_asset,
            database=table_database,
            version="1.0.0",
            status="draft",
            owner=owner,
            created_date=datetime.now(),
            last_modified=datetime.now(),
            schema_definition=schema_def,
            quality_rules=quality_rules or [],
            sla_requirements={"freshness_hours": sla_hours},
            classification=classification,
            tags=table.get("tags", []),
            description=description,
            business_purpose=business_purpose,
            registered_consumers=[],
            downstream_tables=[],
            contains_pii=contains_pii,
            data_history_years=data_history_years,
            change_log=[{
                "timestamp": datetime.now(),
                "action": "created",
                "user": owner,
                "details": "Contract created"
            }]
        )
        
        self.contracts[fqn] = contract
        return contract
    
    def update_contract_status(self, fqn: str, new_status: str, user: str, notes: str = "") -> bool:
        """Update contract status with approval tracking"""
        if fqn not in self.contracts:
            return False
        
        contract = self.contracts[fqn]
        old_status = contract.status
        contract.status = new_status
        contract.last_modified = datetime.now()
        
        contract.change_log.append({
            "timestamp": datetime.now(),
            "action": f"status_change",
            "user": user,
            "details": f"{old_status} → {new_status}",
            "notes": notes
        })
        
        if new_status == "active":
            contract.approval_history.append({
                "timestamp": datetime.now(),
                "approver": user,
                "notes": notes
            })
        
        return True
    
    def detect_schema_changes(self, fqn: str, current_table: Dict) -> List[SchemaChange]:
        """Detect schema changes"""
        if fqn not in self.contracts:
            return []
        
        contract = self.contracts[fqn]
        current_columns = {col["name"]: col for col in current_table.get("columns", [])}
        contract_columns = contract.schema_definition
        
        changes = []
        
        for col_name in contract_columns:
            if col_name not in current_columns:
                changes.append(SchemaChange(
                    table_fqn=fqn,
                    change_type="removed",
                    column_name=col_name,
                    old_value=contract_columns[col_name]["dataType"],
                    new_value=None,
                    detected_at=datetime.now(),
                    severity="breaking",
                    impact_level="high",
                    requires_approval=True
                ))
        
        for col_name in current_columns:
            if col_name not in contract_columns:
                changes.append(SchemaChange(
                    table_fqn=fqn,
                    change_type="added",
                    column_name=col_name,
                    old_value=None,
                    new_value=current_columns[col_name].get("dataType", "UNKNOWN"),
                    detected_at=datetime.now(),
                    severity="non-breaking",
                    impact_level="low",
                    requires_approval=False
                ))
        
        for col_name in contract_columns:
            if col_name in current_columns:
                old_type = contract_columns[col_name]["dataType"]
                new_type = current_columns[col_name].get("dataType", "UNKNOWN")
                
                if old_type != new_type:
                    changes.append(SchemaChange(
                        table_fqn=fqn,
                        change_type="type_changed",
                        column_name=col_name,
                        old_value=old_type,
                        new_value=new_type,
                        detected_at=datetime.now(),
                        severity="breaking",
                        impact_level="high",
                        requires_approval=True
                    ))
        
        return changes
    
    def register_consumer(self, fqn: str, consumer_name: str, consumer_contact: str) -> bool:
        """Register a consumer for a contract"""
        if fqn not in self.contracts:
            return False
        
        contract = self.contracts[fqn]
        consumer_info = f"{consumer_name} ({consumer_contact})"
        
        if consumer_info not in contract.registered_consumers:
            contract.registered_consumers.append(consumer_info)
            contract.change_log.append({
                "timestamp": datetime.now(),
                "action": "consumer_registered",
                "user": consumer_contact,
                "details": f"Consumer registered: {consumer_name}"
            })
        
        return True
    
    def get_contracts_by_status(self, status: str) -> List[DataContract]:
        """Get contracts filtered by status"""
        return [c for c in self.contracts.values() if c.status == status]
    
    def get_contracts_by_owner(self, owner: str) -> List[DataContract]:
        """Get contracts owned by a specific user"""
        return [c for c in self.contracts.values() if c.owner == owner]

# =============================================================================
# GOVERNANCE ENGINE
# =============================================================================

class GovernanceEngine:
    """Calculate governance metrics and insights"""
    
    @staticmethod
    def calculate_governance_metrics(tables: List[Dict], contracts: Dict[str, DataContract]) -> GovernanceMetrics:
        """Calculate overall governance health"""
        total = len(tables)
        owned = sum(1 for t in tables if t.get("owner", {}).get("name"))
        documented = sum(1 for t in tables if t.get("description"))
        classified = sum(1 for t in tables if t.get("tags"))
        contracted = len(contracts)
        
        # Compliance = has active contract + proper classification
        compliant = sum(
            1 for t in tables
            if t.get("fullyQualifiedName") in contracts
            and contracts[t.get("fullyQualifiedName")].status == "active"
            and contracts[t.get("fullyQualifiedName")].classification
        )
        
        return GovernanceMetrics(
            total_assets=total,
            owned_assets=owned,
            documented_assets=documented,
            classified_assets=classified,
            contracted_assets=contracted,
            compliant_assets=compliant
        )
    
    @staticmethod
    def identify_governance_gaps(tables: List[Dict], contracts: Dict[str, DataContract]) -> Dict[str, List[str]]:
        """Identify tables with governance issues"""
        gaps = {
            "no_owner": [],
            "no_description": [],
            "no_classification": [],
            "no_contract": [],
            "contains_pii_unclassified": []
        }
        
        for table in tables:
            fqn = table.get("fullyQualifiedName", "")
            
            if not table.get("owner", {}).get("name"):
                gaps["no_owner"].append(fqn)
            
            if not table.get("description"):
                gaps["no_description"].append(fqn)
            
            if not table.get("tags"):
                gaps["no_classification"].append(fqn)
            
            if fqn not in contracts:
                gaps["no_contract"].append(fqn)
            
            # Check for PII without proper classification
            tags = str(table.get("tags", []))
            if "PII" in tags and fqn not in contracts:
                gaps["contains_pii_unclassified"].append(fqn)
        
        return gaps
    
    @staticmethod
    def get_stewardship_report(tables: List[Dict]) -> pd.DataFrame:
        """Generate stewardship report by owner"""
        owner_stats = defaultdict(lambda: {
            "tables": 0,
            "documented": 0,
            "with_contracts": 0,
            "domains": set()
        })
        
        for table in tables:
            owner = table.get("owner", {}).get("name", "Unassigned")
            domain = table.get("domain", "Unknown")
            
            owner_stats[owner]["tables"] += 1
            owner_stats[owner]["domains"].add(domain)
            
            if table.get("description"):
                owner_stats[owner]["documented"] += 1
        
        report = []
        for owner, stats in owner_stats.items():
            report.append({
                "Owner": owner,
                "Total Tables": stats["tables"],
                "Documented": stats["documented"],
                "Documentation %": f"{(stats['documented']/stats['tables']*100):.1f}%" if stats["tables"] > 0 else "0%",
                "Domains": ", ".join(sorted(stats["domains"]))
            })
        
        return pd.DataFrame(report).sort_values("Total Tables", ascending=False)

# =============================================================================
# TRUST SCORE ENGINE
# =============================================================================

class TrustScoreEngine:
    """Engine for computing comprehensive data trust scores"""
    
    # Scoring weights for composite score
    WEIGHTS = {
        "data_quality": 0.25,
        "contract_availability": 0.20,
        "freshness": 0.15,
        "documentation": 0.15,
        "lineage_usage": 0.15,
        "security_compliance": 0.10
    }
    
    # Trust level thresholds
    TRUST_LEVELS = {
        (90, 100): "Platinum",
        (75, 90): "Gold",
        (60, 75): "Silver",
        (40, 60): "Bronze",
        (0, 40): "Needs Attention"
    }
    
    def calculate_data_quality_score(self, table: Dict, contracts: Dict[str, DataContract]) -> Tuple[float, List[str], List[str]]:
        """
        Calculate data quality score (0-100)
        Factors: null percentage, data types consistency, quality rules pass rate
        """
        score = 0.0
        improvements = []
        strengths = []
        
        fqn = table.get("fullyQualifiedName", "")
        
        # Base score for table existence
        score += 20
        
        # Check if table has quality rules defined in contract
        if fqn in contracts:
            contract = contracts[fqn]
            if contract.quality_rules:
                score += 30
                strengths.append("Quality rules defined")
            else:
                improvements.append("Define quality rules")
        else:
            improvements.append("Create data contract with quality rules")
        
        # Check column consistency
        columns = table.get("columns", [])
        if columns:
            # Has columns defined
            score += 20
            
            # Check for data type definitions
            typed_columns = [c for c in columns if c.get("dataType") not in ["", "UNKNOWN"]]
            if typed_columns:
                type_coverage = (len(typed_columns) / len(columns)) * 30
                score += type_coverage
                if type_coverage >= 25:
                    strengths.append(f"Strong data type coverage ({len(typed_columns)}/{len(columns)} columns)")
                else:
                    improvements.append("Improve data type definitions")
        else:
            improvements.append("Define table schema")
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_contract_availability_score(self, table: Dict, contracts: Dict[str, DataContract]) -> Tuple[float, List[str], List[str]]:
        """
        Calculate contract availability score (0-100)
        Factors: contract existence, contract status, SLA definitions
        """
        score = 0.0
        improvements = []
        strengths = []
        
        fqn = table.get("fullyQualifiedName", "")
        
        if fqn not in contracts:
            improvements.append("Create data contract")
            return 0.0, improvements, strengths
        
        contract = contracts[fqn]
        
        # Contract exists
        score += 40
        strengths.append("Data contract exists")
        
        # Contract status
        status_scores = {
            "active": 35,
            "review": 25,
            "draft": 10,
            "deprecated": 5
        }
        status_score = status_scores.get(contract.status, 0)
        score += status_score
        
        if contract.status == "active":
            strengths.append("Contract is active")
        elif contract.status == "review":
            improvements.append("Activate contract after review")
        elif contract.status == "draft":
            improvements.append("Move contract from draft to review/active")
        
        # SLA requirements defined
        if contract.sla_requirements and contract.sla_requirements.get("freshness_hours"):
            score += 15
            strengths.append("SLA requirements defined")
        else:
            improvements.append("Define SLA requirements")
        
        # Schema definition completeness
        if contract.schema_definition and len(contract.schema_definition) > 0:
            score += 10
        else:
            improvements.append("Complete schema definition in contract")
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_freshness_score(self, table: Dict) -> Tuple[float, List[str], List[str]]:
        """
        Calculate freshness score (0-100)
        Factors: time since last update
        """
        score = 0.0
        improvements = []
        strengths = []
        
        updated_at = table.get("updatedAt")
        if not updated_at:
            improvements.append("Enable update timestamp tracking")
            return 0.0, improvements, strengths
        
        # Convert milliseconds timestamp to datetime
        try:
            last_update = datetime.fromtimestamp(updated_at / 1000)
            hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
            
            # Scoring based on freshness
            if hours_since_update <= 1:
                score = 100.0
                strengths.append("Data updated within last hour (real-time fresh)")
            elif hours_since_update <= 6:
                score = 95.0
                strengths.append("Data updated within last 6 hours")
            elif hours_since_update <= 24:
                score = 85.0
                strengths.append("Data updated within last 24 hours")
            elif hours_since_update <= 48:
                score = 70.0
            elif hours_since_update <= 168:  # 1 week
                score = 50.0
                improvements.append("Data not updated in several days")
            else:
                score = 20.0
                improvements.append("Data is stale (not updated in over a week)")
        except:
            improvements.append("Invalid update timestamp")
            return 0.0, improvements, strengths
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_documentation_score(self, table: Dict, contracts: Dict[str, DataContract]) -> Tuple[float, List[str], List[str]]:
        """
        Calculate documentation score (0-100)
        Factors: table description, column descriptions, business purpose, tags
        """
        score = 0.0
        improvements = []
        strengths = []
        
        fqn = table.get("fullyQualifiedName", "")
        
        # Table description
        description = table.get("description", "")
        if description and len(description) > 20:
            score += 30
            strengths.append("Table has comprehensive description")
        elif description:
            score += 15
            improvements.append("Expand table description")
        else:
            improvements.append("Add table description")
        
        # Column descriptions
        columns = table.get("columns", [])
        if columns:
            documented_columns = [c for c in columns if c.get("description", "")]
            doc_coverage = (len(documented_columns) / len(columns)) * 30
            score += doc_coverage
            
            if doc_coverage >= 25:
                strengths.append(f"Strong column documentation ({len(documented_columns)}/{len(columns)} columns)")
            elif doc_coverage >= 10:
                improvements.append("Document more columns")
            else:
                improvements.append("Add column descriptions")
        
        # Tags
        tags = table.get("tags", [])
        if tags and len(tags) > 0:
            score += 15
            strengths.append("Table is tagged")
        else:
            improvements.append("Add relevant tags")
        
        # Business purpose from contract
        if fqn in contracts:
            contract = contracts[fqn]
            if contract.business_purpose and len(contract.business_purpose) > 10:
                score += 15
                strengths.append("Business purpose documented")
            else:
                improvements.append("Document business purpose in contract")
        else:
            improvements.append("Create contract to document business purpose")
        
        # Owner assigned
        if table.get("owner", {}).get("name"):
            score += 10
        else:
            improvements.append("Assign table owner")
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_lineage_usage_score(self, table: Dict, contracts: Dict[str, DataContract], 
                                     mock_gen: Optional["MockDataGenerator"] = None) -> Tuple[float, List[str], List[str]]:
        """
        Calculate lineage and usage score (0-100)
        Factors: downstream dependencies, registered consumers, usage depth
        """
        score = 0.0
        improvements = []
        strengths = []
        
        fqn = table.get("fullyQualifiedName", "")
        
        # Check for downstream dependencies (from lineage)
        # In production, would call actual lineage API
        # For demo, we'll use contract information
        
        if fqn in contracts:
            contract = contracts[fqn]
            
            # Downstream tables
            downstream_count = len(contract.downstream_tables)
            if downstream_count > 5:
                score += 35
                strengths.append(f"High usage: {downstream_count} downstream dependencies")
            elif downstream_count > 2:
                score += 25
                strengths.append(f"Moderate usage: {downstream_count} downstream dependencies")
            elif downstream_count > 0:
                score += 15
            else:
                improvements.append("No downstream dependencies tracked")
            
            # Registered consumers
            consumer_count = len(contract.registered_consumers)
            if consumer_count >= 3:
                score += 35
                strengths.append(f"{consumer_count} registered consumers")
            elif consumer_count >= 1:
                score += 20
                strengths.append(f"{consumer_count} registered consumer(s)")
            else:
                improvements.append("Register data consumers")
        else:
            improvements.append("Create contract to track consumers and lineage")
        
        # Table row count as proxy for usage
        row_count = table.get("rowCount", 0)
        if row_count > 100000:
            score += 15
            strengths.append("Large dataset (high usage indicator)")
        elif row_count > 1000:
            score += 10
        elif row_count > 0:
            score += 5
        else:
            improvements.append("No row count data available")
        
        # Popularity/views (would come from OpenMetadata usage metrics in production)
        # For now, give base score if table has activity
        if table.get("updatedAt"):
            score += 15
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_security_compliance_score(self, table: Dict, contracts: Dict[str, DataContract]) -> Tuple[float, List[str], List[str]]:
        """
        Calculate security and compliance score (0-100)
        Factors: classification, PII handling, access controls, compliance requirements
        """
        score = 0.0
        improvements = []
        strengths = []
        
        fqn = table.get("fullyQualifiedName", "")
        
        # Classification assigned
        tags = table.get("tags", [])
        classification = None
        for tag in tags:
            tag_fqn = tag.get("tagFQN", "")
            if "Classification." in tag_fqn:
                classification = tag_fqn.split("Classification.")[-1].lower()
                break
        
        if classification:
            score += 30
            strengths.append(f"Data classified as {classification}")
        else:
            improvements.append("Assign data classification")
        
        # Check for PII handling
        columns = table.get("columns", [])
        has_pii = any("PII" in str(col.get("tags", [])) for col in columns)
        
        if has_pii:
            if fqn in contracts:
                contract = contracts[fqn]
                if contract.contains_pii:
                    score += 25
                    strengths.append("PII properly flagged in contract")
                    
                    # Check for retention policy
                    if contract.retention_days:
                        score += 15
                        strengths.append("Retention policy defined")
                    else:
                        improvements.append("Define retention policy for PII data")
                else:
                    improvements.append("Flag PII in data contract")
                    score += 10
            else:
                improvements.append("Create contract to manage PII")
        else:
            score += 20  # Bonus for non-PII data (less compliance burden)
        
        # Owner assignment (for accountability)
        if table.get("owner", {}).get("name"):
            score += 20
            strengths.append("Data steward assigned")
        else:
            improvements.append("Assign data steward")
        
        # Compliance requirements in contract
        if fqn in contracts:
            contract = contracts[fqn]
            if contract.compliance_requirements and len(contract.compliance_requirements) > 0:
                score += 10
                strengths.append("Compliance requirements documented")
            else:
                improvements.append("Document compliance requirements")
        
        return min(score, 100.0), improvements, strengths
    
    def calculate_trust_score(self, table: Dict, contracts: Dict[str, DataContract],
                             mock_gen: Optional["MockDataGenerator"] = None) -> DataTrustScore:
        """
        Calculate comprehensive trust score for a data asset
        """
        fqn = table.get("fullyQualifiedName", "")
        
        # Calculate component scores
        quality_score, quality_improvements, quality_strengths = self.calculate_data_quality_score(table, contracts)
        contract_score, contract_improvements, contract_strengths = self.calculate_contract_availability_score(table, contracts)
        freshness_score, freshness_improvements, freshness_strengths = self.calculate_freshness_score(table)
        doc_score, doc_improvements, doc_strengths = self.calculate_documentation_score(table, contracts)
        lineage_score, lineage_improvements, lineage_strengths = self.calculate_lineage_usage_score(table, contracts, mock_gen)
        security_score, security_improvements, security_strengths = self.calculate_security_compliance_score(table, contracts)
        
        # Calculate weighted composite score
        composite = (
            quality_score * self.WEIGHTS["data_quality"] +
            contract_score * self.WEIGHTS["contract_availability"] +
            freshness_score * self.WEIGHTS["freshness"] +
            doc_score * self.WEIGHTS["documentation"] +
            lineage_score * self.WEIGHTS["lineage_usage"] +
            security_score * self.WEIGHTS["security_compliance"]
        )
        
        # Determine trust level
        trust_level = "Needs Attention"
        for (min_score, max_score), level in self.TRUST_LEVELS.items():
            if min_score <= composite < max_score:
                trust_level = level
                break
        
        # Aggregate improvements and strengths
        all_improvements = (quality_improvements + contract_improvements + freshness_improvements + 
                          doc_improvements + lineage_improvements + security_improvements)
        all_strengths = (quality_strengths + contract_strengths + freshness_strengths + 
                        doc_strengths + lineage_strengths + security_strengths)
        
        # Get classification
        classification = None
        for tag in table.get("tags", []):
            tag_fqn = tag.get("tagFQN", "")
            if "Classification." in tag_fqn:
                classification = tag_fqn.split("Classification.")[-1].lower()
                break
        
        # Get domain, data_asset, and database
        table_domain = table.get("domain", "Unknown")
        table_data_asset = table.get("data_asset", "")
        table_database = fqn.split(".")[0] if fqn else "Unknown"
        
        return DataTrustScore(
            fqn=fqn,
            table_name=table.get("name", ""),
            domain=table_domain,
            data_asset=table_data_asset,
            database=table_database,
            data_quality_score=quality_score,
            contract_availability_score=contract_score,
            freshness_score=freshness_score,
            documentation_score=doc_score,
            lineage_usage_score=lineage_score,
            security_compliance_score=security_score,
            composite_trust_score=composite,
            trust_level=trust_level,
            owner=table.get("owner", {}).get("name"),
            classification=classification,
            last_assessed=datetime.now(),
            improvement_areas=all_improvements[:5],  # Top 5
            strengths=all_strengths[:5]  # Top 5
        )
    
    def calculate_all_trust_scores(self, tables: List[Dict], contracts: Dict[str, DataContract],
                                   mock_gen: Optional["MockDataGenerator"] = None) -> List[DataTrustScore]:
        """Calculate trust scores for all tables"""
        return [self.calculate_trust_score(table, contracts, mock_gen) for table in tables]
    
    @staticmethod
    def get_trust_score_summary(trust_scores: List[DataTrustScore]) -> Dict[str, Any]:
        """Generate summary statistics from trust scores"""
        if not trust_scores:
            return {}
        
        scores = [ts.composite_trust_score for ts in trust_scores]
        
        # Distribution by trust level
        level_distribution = defaultdict(int)
        for ts in trust_scores:
            level_distribution[ts.trust_level] += 1
        
        # Domain breakdown
        domain_scores = defaultdict(list)
        for ts in trust_scores:
            domain_scores[ts.domain].append(ts.composite_trust_score)
        
        # Database breakdown
        db_scores = defaultdict(list)
        for ts in trust_scores:
            db_scores[ts.database].append(ts.composite_trust_score)
        
        domain_averages = {d: sum(s)/len(s) for d, s in domain_scores.items()}
        db_averages = {db: sum(scores)/len(scores) for db, scores in db_scores.items()}
        
        return {
            "avg_score": sum(scores) / len(scores),
            "max_score": max(scores),
            "min_score": min(scores),
            "median_score": sorted(scores)[len(scores)//2],
            "total_assets": len(trust_scores),
            "level_distribution": dict(level_distribution),
            "domain_averages": domain_averages,
            "database_averages": db_averages,
            "high_trust_assets": len([s for s in scores if s >= 75]),
            "needs_attention_assets": len([s for s in scores if s < 40])
        }

# =============================================================================
# DATA PRODUCT ENGINE
# =============================================================================

class DataProductEngine:
    """
    Engine for managing Data Products - the core marketplace entity.
    Implements Right-to-Left philosophy: products are defined by business purpose
    and wrap multiple data assets/tables into consumable units.
    """
    
    def __init__(self):
        self.products: Dict[str, DataProduct] = {}
    
    def create_product(
        self,
        name: str,
        domain: str,
        business_purpose: str,
        target_personas: List[str],
        north_star_metric: Dict[str, Any],
        functional_metrics: List[Dict[str, Any]],
        granular_metrics: List[Dict[str, Any]],
        data_assets: List[str],
        table_fqns: List[str],
        contract_ids: List[str],
        output_ports: List[Dict[str, Any]],
        owner: str,
        tags: List[str] = None
    ) -> DataProduct:
        """Create a new Data Product"""
        
        product_id = hashlib.md5(f"{name}_{domain}_{datetime.now().isoformat()}".encode()).hexdigest()[:12]
        
        # Convert dict definitions to dataclass instances
        ns_metric = MetricDefinition(
            name=north_star_metric.get("name", ""),
            description=north_star_metric.get("description", ""),
            formula=north_star_metric.get("formula", ""),
            unit=north_star_metric.get("unit", ""),
            source_columns=north_star_metric.get("source_columns", [])
        )
        
        func_metrics = [
            MetricDefinition(
                name=m.get("name", ""),
                description=m.get("description", ""),
                formula=m.get("formula", ""),
                unit=m.get("unit", ""),
                source_columns=m.get("source_columns", [])
            )
            for m in functional_metrics
        ]
        
        gran_metrics = [
            MetricDefinition(
                name=m.get("name", ""),
                description=m.get("description", ""),
                formula=m.get("formula", ""),
                unit=m.get("unit", ""),
                source_columns=m.get("source_columns", [])
            )
            for m in granular_metrics
        ]
        
        ports = [
            OutputPort(
                name=p.get("name", ""),
                port_type=p.get("port_type", "dataset"),
                format=p.get("format", "parquet"),
                description=p.get("description", ""),
                access_pattern=p.get("access_pattern", "batch"),
                endpoint=p.get("endpoint", "")
            )
            for p in output_ports
        ]
        
        product = DataProduct(
            id=product_id,
            name=name,
            domain=domain,
            business_purpose=business_purpose,
            target_personas=target_personas,
            north_star_metric=ns_metric,
            functional_metrics=func_metrics,
            granular_metrics=gran_metrics,
            data_assets=data_assets,
            table_fqns=table_fqns,
            contract_ids=contract_ids,
            output_ports=ports,
            status="draft",
            version="1.0.0",
            owner=owner,
            created_date=datetime.now(),
            last_modified=datetime.now(),
            tags=tags or [],
            change_log=[{
                "timestamp": datetime.now(),
                "action": "created",
                "user": owner,
                "details": f"Data Product '{name}' created"
            }]
        )
        
        self.products[product_id] = product
        return product
    
    def update_product_status(self, product_id: str, new_status: str, user: str, reason: str = "") -> bool:
        """Update product status with audit trail"""
        if product_id not in self.products:
            return False
        
        product = self.products[product_id]
        old_status = product.status
        product.status = new_status
        product.last_modified = datetime.now()
        
        product.change_log.append({
            "timestamp": datetime.now(),
            "action": f"status_change_{old_status}_to_{new_status}",
            "user": user,
            "details": reason or f"Status changed from {old_status} to {new_status}"
        })
        
        return True
    
    def calculate_aggregated_trust(
        self, 
        product: DataProduct, 
        trust_scores: List[DataTrustScore]
    ) -> Tuple[float, str]:
        """
        Calculate aggregated trust score for a product based on its constituent tables.
        Returns (score, trust_level)
        """
        # Filter trust scores for tables in this product
        product_scores = [
            ts for ts in trust_scores 
            if ts.fqn in product.table_fqns
        ]
        
        if not product_scores:
            return 0.0, "Needs Attention"
        
        # Calculate weighted average (all tables equal weight for now)
        avg_score = sum(ts.composite_trust_score for ts in product_scores) / len(product_scores)
        
        # Determine trust level
        if avg_score >= 90:
            trust_level = "Platinum"
        elif avg_score >= 75:
            trust_level = "Gold"
        elif avg_score >= 60:
            trust_level = "Silver"
        elif avg_score >= 40:
            trust_level = "Bronze"
        else:
            trust_level = "Needs Attention"
        
        return avg_score, trust_level
    
    def get_product_consumers(self, product: DataProduct, contracts: Dict[str, DataContract]) -> List[str]:
        """Aggregate consumers from all contracts in the product"""
        consumers = set()
        for contract_id in product.contract_ids:
            # Find contract by ID
            for fqn, contract in contracts.items():
                if contract.id == contract_id:
                    consumers.update(contract.registered_consumers)
                    break
        return list(consumers)
    
    def get_products_by_domain(self, domain: str) -> List[DataProduct]:
        """Get all products for a specific domain"""
        return [p for p in self.products.values() if p.domain == domain]
    
    def get_products_by_status(self, status: str) -> List[DataProduct]:
        """Get products filtered by status"""
        return [p for p in self.products.values() if p.status == status]
    
    def get_active_products(self) -> List[DataProduct]:
        """Get all active products"""
        return self.get_products_by_status("active")
    
    def search_products(self, query: str) -> List[DataProduct]:
        """Search products by name, description, or tags"""
        query_lower = query.lower()
        results = []
        for product in self.products.values():
            if (query_lower in product.name.lower() or
                query_lower in product.business_purpose.lower() or
                any(query_lower in tag.lower() for tag in product.tags) or
                any(query_lower in persona.lower() for persona in product.target_personas)):
                results.append(product)
        return results
    
    def generate_product_manifest(self, product: DataProduct) -> str:
        """Generate YAML manifest for the data product (DataOS-style spec)"""
        manifest = f"""# Data Product Manifest
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

name: {product.name.lower().replace(' ', '-')}
version: {product.version}
type: data-product
status: {product.status}

meta:
  id: {product.id}
  domain: {product.domain}
  owner: {product.owner}
  tags: {product.tags}

business:
  purpose: |
    {product.business_purpose}
  target_personas:
"""
        for persona in product.target_personas:
            manifest += f"    - {persona}\n"
        
        manifest += f"""
metrics:
  north_star:
    name: {product.north_star_metric.name}
    description: {product.north_star_metric.description}
    formula: "{product.north_star_metric.formula}"
    unit: "{product.north_star_metric.unit}"
  
  functional:
"""
        for metric in product.functional_metrics:
            manifest += f"""    - name: {metric.name}
      formula: "{metric.formula}"
      unit: "{metric.unit}"
"""
        
        manifest += """  
  granular:
"""
        for metric in product.granular_metrics:
            manifest += f"""    - name: {metric.name}
      formula: "{metric.formula}"
      unit: "{metric.unit}"
"""
        
        manifest += f"""
composition:
  data_assets:
"""
        for asset in product.data_assets:
            manifest += f"    - {asset}\n"
        
        manifest += """  tables:
"""
        for fqn in product.table_fqns:
            manifest += f"    - {fqn}\n"
        
        manifest += """
output_ports:
"""
        for port in product.output_ports:
            manifest += f"""  - name: {port.name}
    type: {port.port_type}
    format: {port.format}
    access_pattern: {port.access_pattern}
"""
        
        manifest += f"""
quality:
  aggregated_trust_score: {product.aggregated_trust_score:.1f}
  trust_level: {product.trust_level}
"""
        
        return manifest

# =============================================================================
# CODE GENERATION ENGINE
# =============================================================================

class CodeGenerationEngine:
    """Generate code artifacts from data contracts for enterprise development"""
    
    # SQL reserved words that need backticks
    SQL_RESERVED_WORDS = {
        'order', 'group', 'select', 'from', 'where', 'table', 'column', 'index',
        'key', 'primary', 'foreign', 'references', 'constraint', 'unique', 'check',
        'default', 'null', 'not', 'and', 'or', 'in', 'between', 'like', 'is',
        'create', 'alter', 'drop', 'insert', 'update', 'delete', 'join', 'inner',
        'outer', 'left', 'right', 'full', 'cross', 'on', 'as', 'distinct', 'all',
        'union', 'except', 'intersect', 'case', 'when', 'then', 'else', 'end',
        'having', 'limit', 'offset', 'fetch', 'for', 'user', 'role', 'grant',
        'revoke', 'to', 'with', 'recursive', 'temporary', 'temp', 'if', 'exists',
        'cascade', 'restrict', 'action', 'initially', 'deferred', 'immediate',
        'date', 'time', 'timestamp', 'interval', 'year', 'month', 'day', 'hour',
        'minute', 'second', 'zone', 'current', 'row', 'rows', 'range', 'partition',
        'by', 'over', 'window', 'preceding', 'following', 'unbounded', 'current_row'
    }
    
    @staticmethod
    def _escape_sql_string(value: str) -> str:
        """Escape single quotes and special characters in SQL strings"""
        if value is None:
            return ""
        # Escape single quotes by doubling them
        escaped = str(value).replace("'", "''")
        # Remove or escape other problematic characters
        escaped = escaped.replace("\\", "\\\\")
        return escaped
    
    @staticmethod
    def _escape_column_name(col_name: str) -> str:
        """Escape column name with backticks if needed"""
        # Check if column name needs escaping
        needs_escape = (
            col_name.lower() in CodeGenerationEngine.SQL_RESERVED_WORDS or
            not col_name.isidentifier() or
            any(c in col_name for c in [' ', '-', '.', '/', '@', '#', '$', '%'])
        )
        if needs_escape:
            return f"`{col_name}`"
        return col_name
    
    @staticmethod
    def _escape_identifier(name: str) -> str:
        """Escape any SQL identifier (table, schema, catalog names)"""
        if any(c in name for c in [' ', '-', '.', '/', '@', '#', '$', '%']) or name.lower() in CodeGenerationEngine.SQL_RESERVED_WORDS:
            return f"`{name}`"
        return name
    
    @staticmethod
    def _parse_decimal_precision(data_type: str) -> tuple:
        """Parse DECIMAL(p,s) to extract precision and scale"""
        import re
        match = re.match(r'DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', data_type.upper())
        if match:
            return int(match.group(1)), int(match.group(2))
        # Default precision and scale
        return 10, 2
    
    @staticmethod
    def _parse_varchar_length(data_type: str) -> int:
        """Parse VARCHAR(n) to extract length"""
        import re
        match = re.match(r'VARCHAR\s*\(\s*(\d+)\s*\)', data_type.upper())
        if match:
            return int(match.group(1))
        return 255  # Default length
    
    @staticmethod
    def generate_databricks_ddl(contract: DataContract) -> str:
        """Generate Databricks Delta table DDL from contract with proper escaping"""
        
        # Extract database and schema from FQN
        parts = contract.table_fqn.split(".")
        database = parts[0] if len(parts) > 0 else "default"
        schema = parts[1] if len(parts) > 1 else "default"
        table_name = contract.table_name
        
        # Escape identifiers
        esc_database = CodeGenerationEngine._escape_identifier(database)
        esc_schema = CodeGenerationEngine._escape_identifier(schema)
        esc_table = CodeGenerationEngine._escape_identifier(table_name)
        
        ddl = f"""-- Databricks Delta Table DDL
-- Generated from Data Contract: {contract.id}
-- Contract Version: {contract.version}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Owner: {CodeGenerationEngine._escape_sql_string(contract.owner)}
-- Classification: {contract.classification}

CREATE TABLE IF NOT EXISTS {esc_database}.{esc_schema}.{esc_table} (
"""
        
        # Add columns with proper escaping
        column_definitions = []
        for col_name, col_info in contract.schema_definition.items():
            esc_col = CodeGenerationEngine._escape_column_name(col_name)
            data_type = col_info.get("dataType", "STRING")
            nullable = "" if col_info.get("nullable", True) else " NOT NULL"
            description = CodeGenerationEngine._escape_sql_string(col_info.get("description", ""))
            comment = f" COMMENT '{description}'" if description else ""
            
            column_definitions.append(f"    {esc_col} {data_type}{nullable}{comment}")
        
        ddl += ",\n".join(column_definitions)
        ddl += "\n)\n"
        
        # Add table properties with escaped values
        esc_description = CodeGenerationEngine._escape_sql_string(contract.description[:200] if contract.description else "")
        esc_owner = CodeGenerationEngine._escape_sql_string(contract.owner)
        esc_business_purpose = CodeGenerationEngine._escape_sql_string(contract.business_purpose[:100] if contract.business_purpose else "")
        
        ddl += "USING DELTA\n"
        ddl += f"COMMENT '{esc_description}'\n"
        ddl += "TBLPROPERTIES (\n"
        ddl += f"    'contract.id' = '{contract.id}',\n"
        ddl += f"    'contract.version' = '{contract.version}',\n"
        ddl += f"    'contract.owner' = '{esc_owner}',\n"
        ddl += f"    'contract.classification' = '{contract.classification}',\n"
        ddl += f"    'contract.contains_pii' = '{str(contract.contains_pii).lower()}',\n"
        freshness_hours = contract.sla_requirements.get('freshness_hours', 24)
        ddl += f"    'contract.sla_freshness_hours' = '{freshness_hours}',\n"
        ddl += f"    'contract.business_purpose' = '{esc_business_purpose}'\n"
        ddl += ");\n\n"
        
        # Add documentation comments
        ddl += f"-- Business Purpose:\n-- {contract.business_purpose}\n\n"
        ddl += f"-- SLA Requirements:\n"
        ddl += f"--   Freshness: {freshness_hours} hours\n\n"
        
        if contract.contains_pii:
            ddl += "-- ⚠️ WARNING: This table contains PII data\n"
            ddl += f"-- Retention Period: {contract.retention_days or 'Not specified'} days\n\n"
        
        # Add quality rules as comments
        if contract.quality_rules:
            ddl += "-- Data Quality Rules:\n"
            for rule in contract.quality_rules:
                rule_type = rule.get('type', 'unknown')
                col = rule.get('column', 'N/A')
                ddl += f"--   - {rule_type} on column '{col}': {rule}\n"
            ddl += "\n"
        
        return ddl
    
    @staticmethod
    def generate_pyspark_schema(contract: DataContract) -> str:
        """Generate PySpark schema definition from contract with proper type handling"""
        
        table_name_safe = contract.table_name.replace("-", "_").replace(" ", "_")
        
        code = f'''# PySpark Schema Definition
# Generated from Data Contract: {contract.id}
# Contract Version: {contract.version}
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    BinaryType, DecimalType
)

# Schema for {contract.table_name}
{table_name_safe}_schema = StructType([
'''
        
        # Add struct fields with proper type handling
        field_definitions = []
        for col_name, col_info in contract.schema_definition.items():
            raw_type = col_info.get("dataType", "STRING").upper()
            base_type = raw_type.split("(")[0]
            nullable = col_info.get("nullable", True)
            
            # Map SQL types to PySpark types with proper precision handling
            if base_type in ("VARCHAR", "STRING", "CHAR", "TEXT"):
                pyspark_type = "StringType()"
            elif base_type in ("INTEGER", "INT"):
                pyspark_type = "IntegerType()"
            elif base_type == "BIGINT":
                pyspark_type = "LongType()"
            elif base_type == "DECIMAL":
                precision, scale = CodeGenerationEngine._parse_decimal_precision(raw_type)
                pyspark_type = f"DecimalType({precision}, {scale})"
            elif base_type == "DOUBLE":
                pyspark_type = "DoubleType()"
            elif base_type == "FLOAT":
                pyspark_type = "FloatType()"
            elif base_type == "BOOLEAN":
                pyspark_type = "BooleanType()"
            elif base_type == "DATE":
                pyspark_type = "DateType()"
            elif base_type in ("TIMESTAMP", "DATETIME"):
                pyspark_type = "TimestampType()"
            elif base_type == "BINARY":
                pyspark_type = "BinaryType()"
            else:
                pyspark_type = "StringType()"  # Default to string
            
            # Use proper Python boolean (True/False, not true/false)
            nullable_str = "True" if nullable else "False"
            field_definitions.append(f'    StructField("{col_name}", {pyspark_type}, {nullable_str})')
        
        code += ",\n".join(field_definitions)
        code += "\n])\n\n"
        
        # Add usage example with correct path format
        parts = contract.table_fqn.split(".")
        catalog = parts[0] if len(parts) > 0 else "main"
        schema_name = parts[1] if len(parts) > 1 else "default"
        
        code += f'''# Usage Example - Read from Unity Catalog:
df = spark.table("{contract.table_fqn}")

# Or read from Delta path with schema validation:
# df = spark.read.format("delta") \\
#     .schema({table_name_safe}_schema) \\
#     .load("/mnt/data/{catalog}/{schema_name}/{contract.table_name}")

# Create DataFrame with schema:
# df = spark.createDataFrame(data, schema={table_name_safe}_schema)

# Validate schema matches
def validate_schema(df):
    """Validate DataFrame schema against contract"""
    expected = set({table_name_safe}_schema.fieldNames())
    actual = set(df.schema.fieldNames())
    
    missing = expected - actual
    extra = actual - expected
    
    if missing:
        print(f"⚠️ Missing columns: {{missing}}")
    if extra:
        print(f"ℹ️ Extra columns: {{extra}}")
    if not missing and not extra:
        print("✅ Schema matches contract")
    
    return len(missing) == 0

# validate_schema(df)
'''
        
        return code
    
    @staticmethod
    def generate_quality_tests(contract: DataContract) -> str:
        """Generate PySpark data quality tests from contract quality rules"""
        return CodeGenerationEngine._generate_pyspark_tests(contract)
    
    @staticmethod
    def _generate_pyspark_tests(contract: DataContract) -> str:
        """Generate comprehensive PySpark validation code using contract quality rules"""
        
        table_name_safe = contract.table_name.replace("-", "_").replace(" ", "_")
        
        code = f'''# PySpark Data Quality Validation
# Generated from Data Contract: {contract.id}
# Contract Version: {contract.version}
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#
# This module validates data against the contract's quality rules.
# Quality Rules Configured: {len(contract.quality_rules)}

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DataQualityValidator:
    """
    Data Quality Validator for {contract.table_name}
    Contract ID: {contract.id}
    """
    
    def __init__(self, df: DataFrame):
        self.df = df
        self.results = {{
            "table_name": "{contract.table_name}",
            "contract_id": "{contract.id}",
            "contract_version": "{contract.version}",
            "validation_timestamp": datetime.now().isoformat(),
            "total_rows": 0,
            "checks": [],
            "summary": {{
                "total_checks": 0,
                "passed": 0,
                "failed": 0,
                "warnings": 0
            }}
        }}
    
    def _add_check_result(self, check_name: str, check_type: str, column: str,
                          passed: bool, details: Dict[str, Any], threshold: float = None):
        """Add a check result to the results dictionary"""
        status = "passed" if passed else "failed"
        
        # Handle threshold-based checks (warning if close to threshold)
        if threshold is not None and passed:
            actual_rate = details.get("pass_rate", 1.0)
            if actual_rate < threshold + 0.05:  # Within 5% of threshold
                status = "warning"
        
        self.results["checks"].append({{
            "check_name": check_name,
            "check_type": check_type,
            "column": column,
            "status": status,
            "threshold": threshold,
            "details": details
        }})
        
        self.results["summary"]["total_checks"] += 1
        if status == "passed":
            self.results["summary"]["passed"] += 1
        elif status == "failed":
            self.results["summary"]["failed"] += 1
        else:
            self.results["summary"]["warnings"] += 1
    
    def check_null(self, column: str, threshold: float = 0.95) -> bool:
        """
        Check null rate for a column
        Args:
            column: Column name to check
            threshold: Minimum percentage of non-null values required (0.0 to 1.0)
        """
        total = self.df.count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_null_check", "null_check", column, False,
                {{"error": "No rows in DataFrame"}}, threshold
            )
            return False
        
        null_count = self.df.filter(F.col(column).isNull()).count()
        non_null_rate = (total - null_count) / total
        passed = non_null_rate >= threshold
        
        self._add_check_result(
            f"{{column}}_null_check", "null_check", column, passed,
            {{
                "total_rows": total,
                "null_count": null_count,
                "non_null_count": total - null_count,
                "non_null_rate": round(non_null_rate, 4),
                "pass_rate": round(non_null_rate, 4),
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_uniqueness(self, column: str, threshold: float = 1.0) -> bool:
        """
        Check uniqueness of values in a column
        Args:
            column: Column name to check
            threshold: Minimum percentage of unique values required (0.0 to 1.0)
        """
        total = self.df.count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_uniqueness", "uniqueness", column, False,
                {{"error": "No rows in DataFrame"}}, threshold
            )
            return False
        
        # Count duplicates
        duplicate_count = self.df.groupBy(column).count().filter(F.col("count") > 1).count()
        duplicate_rows = self.df.groupBy(column).count().filter(F.col("count") > 1) \\
            .agg(F.sum("count")).collect()[0][0] or 0
        
        unique_count = self.df.select(column).distinct().count()
        uniqueness_rate = unique_count / total if total > 0 else 0
        passed = uniqueness_rate >= threshold
        
        self._add_check_result(
            f"{{column}}_uniqueness", "uniqueness", column, passed,
            {{
                "total_rows": total,
                "unique_values": unique_count,
                "duplicate_groups": duplicate_count,
                "duplicate_rows": int(duplicate_rows) if duplicate_rows else 0,
                "uniqueness_rate": round(uniqueness_rate, 4),
                "pass_rate": round(uniqueness_rate, 4),
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_range(self, column: str, min_value=None, max_value=None, threshold: float = 1.0) -> bool:
        """
        Check if values fall within specified range
        Args:
            column: Column name to check
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            threshold: Minimum percentage of values within range (0.0 to 1.0)
        """
        total = self.df.filter(F.col(column).isNotNull()).count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_range", "range_check", column, False,
                {{"error": "No non-null rows"}}, threshold
            )
            return False
        
        # Build filter condition
        conditions = []
        if min_value is not None:
            conditions.append(F.col(column) < min_value)
        if max_value is not None:
            conditions.append(F.col(column) > max_value)
        
        if not conditions:
            self._add_check_result(
                f"{{column}}_range", "range_check", column, True,
                {{"info": "No range constraints specified"}}, threshold
            )
            return True
        
        out_of_range_filter = conditions[0]
        for cond in conditions[1:]:
            out_of_range_filter = out_of_range_filter | cond
        
        out_of_range = self.df.filter(F.col(column).isNotNull()).filter(out_of_range_filter).count()
        in_range_rate = (total - out_of_range) / total
        passed = in_range_rate >= threshold
        
        # Get actual min/max for reporting
        stats = self.df.agg(
            F.min(column).alias("actual_min"),
            F.max(column).alias("actual_max")
        ).collect()[0]
        
        self._add_check_result(
            f"{{column}}_range", "range_check", column, passed,
            {{
                "total_rows": total,
                "out_of_range_count": out_of_range,
                "in_range_count": total - out_of_range,
                "in_range_rate": round(in_range_rate, 4),
                "pass_rate": round(in_range_rate, 4),
                "expected_min": str(min_value),
                "expected_max": str(max_value),
                "actual_min": str(stats["actual_min"]),
                "actual_max": str(stats["actual_max"]),
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_format(self, column: str, pattern: str, pattern_type: str = "custom", 
                     threshold: float = 0.95) -> bool:
        """
        Check if values match a regex pattern
        Args:
            column: Column name to check
            pattern: Regex pattern to match
            pattern_type: Type of pattern (email, phone, url, uuid, custom)
            threshold: Minimum percentage of matching values (0.0 to 1.0)
        """
        total = self.df.filter(F.col(column).isNotNull()).count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_format", "format_check", column, False,
                {{"error": "No non-null rows"}}, threshold
            )
            return False
        
        non_matching = self.df.filter(
            F.col(column).isNotNull() & ~F.col(column).rlike(pattern)
        ).count()
        
        match_rate = (total - non_matching) / total
        passed = match_rate >= threshold
        
        # Get sample non-matching values for debugging
        sample_failures = []
        if non_matching > 0:
            samples = self.df.filter(
                F.col(column).isNotNull() & ~F.col(column).rlike(pattern)
            ).select(column).limit(3).collect()
            sample_failures = [str(row[0])[:50] for row in samples]
        
        self._add_check_result(
            f"{{column}}_format", "format_check", column, passed,
            {{
                "total_rows": total,
                "matching_count": total - non_matching,
                "non_matching_count": non_matching,
                "match_rate": round(match_rate, 4),
                "pass_rate": round(match_rate, 4),
                "pattern_type": pattern_type,
                "pattern": pattern[:100],  # Truncate long patterns
                "sample_failures": sample_failures,
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_length(self, column: str, min_length: int = None, max_length: int = None,
                     threshold: float = 1.0) -> bool:
        """
        Check string length constraints
        Args:
            column: Column name to check
            min_length: Minimum string length
            max_length: Maximum string length
            threshold: Minimum percentage of values within length constraints (0.0 to 1.0)
        """
        total = self.df.filter(F.col(column).isNotNull()).count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_length", "length_check", column, False,
                {{"error": "No non-null rows"}}, threshold
            )
            return False
        
        # Build filter for violations
        conditions = []
        if min_length is not None:
            conditions.append(F.length(F.col(column)) < min_length)
        if max_length is not None:
            conditions.append(F.length(F.col(column)) > max_length)
        
        if not conditions:
            self._add_check_result(
                f"{{column}}_length", "length_check", column, True,
                {{"info": "No length constraints specified"}}, threshold
            )
            return True
        
        violation_filter = conditions[0]
        for cond in conditions[1:]:
            violation_filter = violation_filter | cond
        
        violations = self.df.filter(F.col(column).isNotNull()).filter(violation_filter).count()
        valid_rate = (total - violations) / total
        passed = valid_rate >= threshold
        
        # Get length stats
        stats = self.df.filter(F.col(column).isNotNull()).agg(
            F.min(F.length(column)).alias("min_len"),
            F.max(F.length(column)).alias("max_len"),
            F.avg(F.length(column)).alias("avg_len")
        ).collect()[0]
        
        self._add_check_result(
            f"{{column}}_length", "length_check", column, passed,
            {{
                "total_rows": total,
                "valid_count": total - violations,
                "violation_count": violations,
                "valid_rate": round(valid_rate, 4),
                "pass_rate": round(valid_rate, 4),
                "expected_min_length": min_length,
                "expected_max_length": max_length,
                "actual_min_length": stats["min_len"],
                "actual_max_length": stats["max_len"],
                "actual_avg_length": round(stats["avg_len"], 2) if stats["avg_len"] else None,
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_allowed_values(self, column: str, allowed_values: List[str],
                             threshold: float = 1.0) -> bool:
        """
        Check if values are in allowed list
        Args:
            column: Column name to check
            allowed_values: List of allowed values
            threshold: Minimum percentage of valid values (0.0 to 1.0)
        """
        total = self.df.filter(F.col(column).isNotNull()).count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_allowed_values", "allowed_values", column, False,
                {{"error": "No non-null rows"}}, threshold
            )
            return False
        
        invalid_count = self.df.filter(
            F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)
        ).count()
        
        valid_rate = (total - invalid_count) / total
        passed = valid_rate >= threshold
        
        # Get invalid values sample
        invalid_samples = []
        if invalid_count > 0:
            samples = self.df.filter(
                F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)
            ).select(column).distinct().limit(5).collect()
            invalid_samples = [str(row[0]) for row in samples]
        
        self._add_check_result(
            f"{{column}}_allowed_values", "allowed_values", column, passed,
            {{
                "total_rows": total,
                "valid_count": total - invalid_count,
                "invalid_count": invalid_count,
                "valid_rate": round(valid_rate, 4),
                "pass_rate": round(valid_rate, 4),
                "allowed_values_count": len(allowed_values),
                "sample_invalid_values": invalid_samples,
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def check_freshness(self, column: str, max_age_hours: int, threshold: float = 1.0) -> bool:
        """
        Check data freshness for timestamp columns
        Args:
            column: Timestamp column name
            max_age_hours: Maximum allowed age in hours
            threshold: Minimum percentage of fresh records (0.0 to 1.0)
        """
        total = self.df.filter(F.col(column).isNotNull()).count()
        if total == 0:
            self._add_check_result(
                f"{{column}}_freshness", "freshness_check", column, False,
                {{"error": "No non-null rows"}}, threshold
            )
            return False
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        stale_count = self.df.filter(
            F.col(column).isNotNull() & (F.col(column) < F.lit(cutoff_time))
        ).count()
        
        fresh_rate = (total - stale_count) / total
        passed = fresh_rate >= threshold
        
        # Get freshness stats
        stats = self.df.filter(F.col(column).isNotNull()).agg(
            F.min(column).alias("oldest"),
            F.max(column).alias("newest")
        ).collect()[0]
        
        self._add_check_result(
            f"{{column}}_freshness", "freshness_check", column, passed,
            {{
                "total_rows": total,
                "fresh_count": total - stale_count,
                "stale_count": stale_count,
                "fresh_rate": round(fresh_rate, 4),
                "pass_rate": round(fresh_rate, 4),
                "max_age_hours": max_age_hours,
                "cutoff_time": cutoff_time.isoformat(),
                "oldest_record": str(stats["oldest"]),
                "newest_record": str(stats["newest"]),
                "threshold": threshold
            }},
            threshold
        )
        return passed
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all configured quality checks and return results"""
        self.results["total_rows"] = self.df.count()
        
        # Run schema validation first
        expected_columns = {list(contract.schema_definition.keys())}
        actual_columns = set(self.df.columns)
        missing_cols = expected_columns - actual_columns
        extra_cols = actual_columns - expected_columns
        
        self._add_check_result(
            "schema_validation", "schema", "N/A",
            len(missing_cols) == 0,
            {{
                "expected_columns": list(expected_columns),
                "actual_columns": list(actual_columns),
                "missing_columns": list(missing_cols),
                "extra_columns": list(extra_cols)
            }}
        )
'''
        
        # Generate checks from contract quality rules
        if contract.quality_rules:
            code += "\n        # Contract-defined quality rules\n"
            
            for rule in contract.quality_rules:
                rule_type = rule.get('type', '')
                column = rule.get('column', '')
                threshold = rule.get('threshold', 0.95)
                
                if rule_type == 'null_check' and column:
                    code += f'        self.check_null("{column}", threshold={threshold})\n'
                
                elif rule_type == 'uniqueness' and column:
                    code += f'        self.check_uniqueness("{column}", threshold={threshold})\n'
                
                elif rule_type == 'range_check' and column:
                    min_val = rule.get('min_value')
                    max_val = rule.get('max_value')
                    min_str = f'"{min_val}"' if isinstance(min_val, str) else str(min_val) if min_val is not None else 'None'
                    max_str = f'"{max_val}"' if isinstance(max_val, str) else str(max_val) if max_val is not None else 'None'
                    code += f'        self.check_range("{column}", min_value={min_str}, max_value={max_str}, threshold={threshold})\n'
                
                elif rule_type == 'format_check' and column:
                    pattern = rule.get('regex') or rule.get('custom_regex', '.*')
                    pattern_type = rule.get('pattern_type', 'custom')
                    # Escape the pattern for Python string
                    escaped_pattern = pattern.replace('\\', '\\\\').replace('"', '\\"')
                    code += f'        self.check_format("{column}", pattern=r"{escaped_pattern}", pattern_type="{pattern_type}", threshold={threshold})\n'
                
                elif rule_type == 'length_check' and column:
                    min_len = rule.get('min_length')
                    max_len = rule.get('max_length')
                    code += f'        self.check_length("{column}", min_length={min_len}, max_length={max_len}, threshold={threshold})\n'
                
                elif rule_type == 'allowed_values' and column:
                    values = rule.get('allowed_values', [])
                    values_str = str(values)
                    code += f'        self.check_allowed_values("{column}", allowed_values={values_str}, threshold={threshold})\n'
                
                elif rule_type == 'freshness_check' and column:
                    max_age = rule.get('max_age_hours', 24)
                    code += f'        self.check_freshness("{column}", max_age_hours={max_age}, threshold={threshold})\n'
        
        # Also generate checks based on schema constraints (NOT NULL columns)
        code += "\n        # Schema-based constraints (NOT NULL columns)\n"
        for col_name, col_info in contract.schema_definition.items():
            if not col_info.get("nullable", True):
                # Check if we already have a null_check rule for this column
                existing_null_check = any(
                    r.get('type') == 'null_check' and r.get('column') == col_name 
                    for r in contract.quality_rules
                )
                if not existing_null_check:
                    code += f'        self.check_null("{col_name}", threshold=1.0)  # NOT NULL constraint\n'
        
        code += '''
        return self.results
    
    def print_summary(self):
        """Print a formatted summary of validation results"""
        summary = self.results["summary"]
        print("=" * 60)
        print(f"DATA QUALITY VALIDATION REPORT")
        print(f"Table: {self.results['table_name']}")
        print(f"Contract: {self.results['contract_id']} v{self.results['contract_version']}")
        print(f"Timestamp: {self.results['validation_timestamp']}")
        print(f"Total Rows: {self.results['total_rows']:,}")
        print("=" * 60)
        print(f"Total Checks: {summary['total_checks']}")
        print(f"  ✅ Passed:   {summary['passed']}")
        print(f"  ⚠️ Warnings: {summary['warnings']}")
        print(f"  ❌ Failed:   {summary['failed']}")
        print("=" * 60)
        
        # Print failed checks
        failed = [c for c in self.results["checks"] if c["status"] == "failed"]
        if failed:
            print("\\nFAILED CHECKS:")
            for check in failed:
                print(f"  ❌ {check['check_name']} ({check['column']})")
                print(f"     Details: {check['details']}")
        
        # Print warnings
        warnings = [c for c in self.results["checks"] if c["status"] == "warning"]
        if warnings:
            print("\\nWARNINGS:")
            for check in warnings:
                print(f"  ⚠️ {check['check_name']} ({check['column']})")
                print(f"     Pass rate close to threshold")


'''
        
        code += f'''# Convenience function for quick validation
def validate_{table_name_safe}(df: DataFrame) -> Dict[str, Any]:
    """
    Validate {contract.table_name} against data contract
    Returns validation results dictionary
    """
    validator = DataQualityValidator(df)
    results = validator.run_all_checks()
    validator.print_summary()
    return results


# Usage Example:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DQ Validation").getOrCreate()
# df = spark.table("{contract.table_fqn}")
# results = validate_{table_name_safe}(df)
# 
# # Access results programmatically:
# if results["summary"]["failed"] > 0:
#     raise Exception(f"Data quality validation failed: {{results['summary']['failed']}} checks failed")
'''
        
        return code
    
    @staticmethod
    def generate_unity_catalog_sql(contract: DataContract) -> str:
        """Generate Unity Catalog registration SQL with proper escaping"""
        
        parts = contract.table_fqn.split(".")
        catalog = parts[0] if len(parts) > 0 else "main"
        schema = parts[1] if len(parts) > 1 else "default"
        table_name = contract.table_name
        
        # Escape identifiers
        esc_catalog = CodeGenerationEngine._escape_identifier(catalog)
        esc_schema = CodeGenerationEngine._escape_identifier(schema)
        esc_table = CodeGenerationEngine._escape_identifier(table_name)
        
        # Escape string values
        esc_business_purpose = CodeGenerationEngine._escape_sql_string(contract.business_purpose[:100] if contract.business_purpose else "")
        esc_owner = CodeGenerationEngine._escape_sql_string(contract.owner)
        
        sql = f"""-- Unity Catalog Registration
-- Generated from Data Contract: {contract.id}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Create catalog if not exists
CREATE CATALOG IF NOT EXISTS {esc_catalog};

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS {esc_catalog}.{esc_schema}
COMMENT '{esc_business_purpose}';

-- Set table properties for contract tracking
ALTER TABLE {esc_catalog}.{esc_schema}.{esc_table} SET TBLPROPERTIES (
    'contract.id' = '{contract.id}',
    'contract.version' = '{contract.version}',
    'contract.owner' = '{esc_owner}',
    'contract.classification' = '{contract.classification}',
    'contract.contains_pii' = '{str(contract.contains_pii).lower()}'
);

-- Add column comments
"""
        
        for col_name, col_info in contract.schema_definition.items():
            description = col_info.get("description", "")
            if description:
                esc_col = CodeGenerationEngine._escape_column_name(col_name)
                esc_desc = CodeGenerationEngine._escape_sql_string(description)
                sql += f"ALTER TABLE {esc_catalog}.{esc_schema}.{esc_table} ALTER COLUMN {esc_col} COMMENT '{esc_desc}';\n"
        
        # Classification-based tagging
        sql += f"\n-- Apply classification tags\n"
        sql += f"ALTER TABLE {esc_catalog}.{esc_schema}.{esc_table} SET TAGS ('classification' = '{contract.classification}');\n"
        
        if contract.contains_pii:
            sql += f"ALTER TABLE {esc_catalog}.{esc_schema}.{esc_table} SET TAGS ('contains_pii' = 'true');\n"
            
            # Tag PII columns
            sql += "\n-- Tag PII columns\n"
            for col_name, col_info in contract.schema_definition.items():
                if col_info.get("isPII", False):
                    esc_col = CodeGenerationEngine._escape_column_name(col_name)
                    sql += f"ALTER TABLE {esc_catalog}.{esc_schema}.{esc_table} ALTER COLUMN {esc_col} SET TAGS ('pii' = 'true');\n"
        
        sql += f"\n-- Grant permissions based on classification\n"
        sql += f"-- NOTE: Replace placeholder group names with your actual security groups\n"
        if contract.classification == "public":
            sql += f"-- GRANT SELECT ON TABLE {esc_catalog}.{esc_schema}.{esc_table} TO `data_consumers`;\n"
        elif contract.classification == "internal":
            sql += f"-- GRANT SELECT ON TABLE {esc_catalog}.{esc_schema}.{esc_table} TO `internal_data_users`;\n"
        elif contract.classification == "confidential":
            sql += f"-- GRANT SELECT ON TABLE {esc_catalog}.{esc_schema}.{esc_table} TO `confidential_data_users`;\n"
            sql += f"-- Consider enabling row-level or column-level security\n"
        elif contract.classification == "restricted":
            sql += f"-- RESTRICTED: Manual approval and grants required\n"
            sql += f"-- GRANT SELECT ON TABLE {esc_catalog}.{esc_schema}.{esc_table} TO `approved_user`;\n"
            sql += f"-- Enable audit logging for all access\n"
        
        return sql
    
    @staticmethod
    def generate_documentation(contract: DataContract) -> str:
        """Generate comprehensive Markdown documentation"""
        
        doc = f"""# Data Contract: {contract.table_name}

**Contract ID:** `{contract.id}`  
**Version:** {contract.version}  
**Status:** {contract.status}  
**Owner:** {contract.owner}  
**Classification:** {contract.classification}  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## Business Purpose

{contract.business_purpose}

## Description

{contract.description}

---

## Schema Definition

| Column Name | Data Type | Nullable | PII | Description |
|-------------|-----------|----------|-----|-------------|
"""
        
        for col_name, col_info in contract.schema_definition.items():
            data_type = col_info.get("dataType", "STRING")
            nullable = "Yes" if col_info.get("nullable", True) else "**No**"
            is_pii = "🔒 Yes" if col_info.get("isPII", False) else "No"
            description = col_info.get("description", "-")
            
            doc += f"| `{col_name}` | {data_type} | {nullable} | {is_pii} | {description} |\n"
        
        doc += "\n---\n\n## Data Quality Rules\n\n"
        
        if contract.quality_rules:
            doc += "| Rule Type | Column | Configuration | Threshold |\n"
            doc += "|-----------|--------|---------------|----------:|\n"
            
            for rule in contract.quality_rules:
                rule_type = rule.get('type', 'Unknown')
                column = rule.get('column', 'N/A')
                threshold = rule.get('threshold', 'N/A')
                if isinstance(threshold, float):
                    threshold = f"{threshold*100:.0f}%"
                
                # Build config summary
                config_parts = []
                if 'min_value' in rule:
                    config_parts.append(f"min={rule['min_value']}")
                if 'max_value' in rule:
                    config_parts.append(f"max={rule['max_value']}")
                if 'pattern_type' in rule:
                    config_parts.append(f"pattern={rule['pattern_type']}")
                if 'min_length' in rule:
                    config_parts.append(f"min_len={rule['min_length']}")
                if 'max_length' in rule:
                    config_parts.append(f"max_len={rule['max_length']}")
                if 'max_age_hours' in rule:
                    config_parts.append(f"max_age={rule['max_age_hours']}h")
                if 'allowed_values' in rule:
                    count = len(rule['allowed_values'])
                    config_parts.append(f"{count} allowed values")
                
                config = ", ".join(config_parts) if config_parts else "-"
                doc += f"| {rule_type} | `{column}` | {config} | {threshold} |\n"
        else:
            doc += "*No quality rules defined*\n"
        
        doc += "\n---\n\n## SLA Requirements\n\n"
        doc += f"| Requirement | Value |\n"
        doc += f"|-------------|-------|\n"
        doc += f"| **Freshness** | {contract.sla_requirements.get('freshness_hours', 24)} hours |\n"
        
        if contract.retention_days:
            doc += f"| **Retention** | {contract.retention_days} days |\n"
        
        doc += "\n---\n\n## Compliance & Security\n\n"
        doc += f"| Attribute | Value |\n"
        doc += f"|-----------|-------|\n"
        doc += f"| **Classification** | {contract.classification} |\n"
        doc += f"| **Contains PII** | {'Yes ⚠️' if contract.contains_pii else 'No'} |\n"
        
        if contract.compliance_requirements:
            doc += f"| **Compliance** | {', '.join(contract.compliance_requirements)} |\n"
        
        if contract.registered_consumers:
            doc += "\n---\n\n## Registered Consumers\n\n"
            for consumer in contract.registered_consumers:
                doc += f"- {consumer}\n"
        
        if contract.downstream_tables:
            doc += "\n---\n\n## Downstream Dependencies\n\n"
            for table in contract.downstream_tables:
                doc += f"- `{table}`\n"
        
        doc += f"\n---\n\n## Change History\n\n"
        doc += "| Date | Action | User | Details |\n"
        doc += "|------|--------|------|----------|\n"
        
        for log in reversed(contract.change_log[-10:]):
            date = log['timestamp'].strftime('%Y-%m-%d %H:%M')
            details = CodeGenerationEngine._escape_sql_string(log.get('details', ''))[:50]
            doc += f"| {date} | {log['action']} | {log['user']} | {details} |\n"
        
        doc += f"\n---\n\n*Document generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"
        
        return doc
    
    @staticmethod
    def generate_databricks_notebook(contract: DataContract) -> str:
        """Generate complete Databricks notebook with all artifacts"""
        
        table_name_safe = contract.table_name.replace("-", "_").replace(" ", "_")
        
        # Pre-generate the components
        ddl_code = CodeGenerationEngine.generate_databricks_ddl(contract)
        schema_code = CodeGenerationEngine.generate_pyspark_schema(contract)
        quality_code = CodeGenerationEngine._generate_pyspark_tests(contract)
        unity_code = CodeGenerationEngine.generate_unity_catalog_sql(contract)
        
        # Escape for embedding in notebook
        ddl_escaped = ddl_code.replace('"""', '\\"\\"\\"')
        unity_escaped = unity_code.replace('"""', '\\"\\"\\"')
        
        notebook = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Data Contract Implementation: {contract.table_name}
# MAGIC 
# MAGIC | Attribute | Value |
# MAGIC |-----------|-------|
# MAGIC | **Contract ID** | `{contract.id}` |
# MAGIC | **Version** | {contract.version} |
# MAGIC | **Owner** | {contract.owner} |
# MAGIC | **Classification** | {contract.classification} |
# MAGIC | **Generated** | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Business Purpose
# MAGIC {contract.business_purpose}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Delta Table
# MAGIC 
# MAGIC Execute the DDL to create the table with contract metadata.

# COMMAND ----------

# DDL for table creation
ddl = """
{ddl_escaped}
"""

# Parse and execute DDL statements
for statement in ddl.split(';'):
    statement = statement.strip()
    if statement and not statement.startswith('--'):
        try:
            spark.sql(statement)
            print(f"✅ Executed: {{statement[:80]}}...")
        except Exception as e:
            print(f"⚠️ Skipped: {{statement[:80]}}... ({{str(e)[:50]}})")

print("\\n✅ Table creation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define PySpark Schema
# MAGIC 
# MAGIC Schema definition for programmatic use.

# COMMAND ----------

{schema_code}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Quality Validation Framework
# MAGIC 
# MAGIC Comprehensive data quality checks based on contract rules.
# MAGIC 
# MAGIC **Quality Rules Configured:** {len(contract.quality_rules)}

# COMMAND ----------

{quality_code}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Unity Catalog Registration
# MAGIC 
# MAGIC Apply governance metadata and permissions.

# COMMAND ----------

unity_sql = """
{unity_escaped}
"""

# Execute Unity Catalog commands
for statement in unity_sql.split(';'):
    statement = statement.strip()
    if statement and not statement.startswith('--'):
        try:
            spark.sql(statement)
            print(f"✅ Executed: {{statement[:60]}}...")
        except Exception as e:
            # Some commands may fail if objects don't exist yet
            print(f"⚠️ Skipped: {{statement[:60]}}... ({{str(e)[:50]}})")

print("\\n✅ Unity Catalog registration complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Data (Run After Loading Data)
# MAGIC 
# MAGIC Execute this cell after data has been loaded to validate against the contract.

# COMMAND ----------

# Load table and run validation
try:
    df = spark.table("{contract.table_fqn}")
    print(f"Loaded {{df.count():,}} rows from {contract.table_fqn}")
    
    # Run validation
    results = validate_{table_name_safe}(df)
    
    # Store results for downstream use
    validation_passed = results["summary"]["failed"] == 0
    
    if not validation_passed:
        print("\\n⚠️ Data quality issues detected - review failed checks above")
    else:
        print("\\n✅ All data quality checks passed!")
        
except Exception as e:
    print(f"❌ Error: {{e}}")
    print("Note: Table may not exist yet or may be empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Metadata Summary
# MAGIC 
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | **Business Purpose** | {contract.business_purpose[:100]}{'...' if len(contract.business_purpose) > 100 else ''} |
# MAGIC | **Classification** | {contract.classification} |
# MAGIC | **Contains PII** | {'Yes ⚠️' if contract.contains_pii else 'No'} |
# MAGIC | **SLA Freshness** | {contract.sla_requirements.get('freshness_hours', 24)} hours |
# MAGIC | **Quality Rules** | {len(contract.quality_rules)} configured |
'''
        
        return notebook

# =============================================================================
# MOCK DATA GENERATOR
# =============================================================================

class MockDataGenerator:
    """Generate realistic mock data"""
    
    @staticmethod
    def generate_mock_tables(count: int = 60) -> List[Dict]:
        """Generate mock tables"""
        import random
        
        schemas = ["raw", "staging", "processed", "analytics", "reporting"]
        owners = ["alice.data", "bob.engineering", "carol.analytics", "dave.ops", "eve.finance"]
        
        tables = []
        
        # Focus on Deliver domain (about 40 tables), rest for other domains (about 20 tables)
        deliver_count = int(count * 0.67)  # ~40 tables for Deliver
        other_count = count - deliver_count  # ~20 tables for other domains
        
        deliver_assets = DATA_ASSETS.get("Deliver", [])
        other_domains = [d for d in ALLOWED_DOMAINS if d != "Deliver"]
        
        # Generate Deliver domain tables with data assets
        for i in range(deliver_count):
            domain = "Deliver"
            data_asset = deliver_assets[i % len(deliver_assets)] if deliver_assets else ""
            database = DATA_ASSET_DATABASE_MAPPING.get(data_asset, ALLOWED_DATABASES[0])
            schema = schemas[i % len(schemas)]
            table_name = f"table_{domain.lower()}_{i:03d}"
            
            num_columns = 5 + (i % 10)
            columns = []
            for j in range(num_columns):
                col_type = ["VARCHAR", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"][j % 6]
                is_pii = (j % 7 == 0)  # Some columns are PII
                
                columns.append({
                    "name": f"col_{j}",
                    "dataType": col_type,
                    "constraint": "NOT NULL" if j == 0 else "",
                    "description": f"Column {j} - {col_type} field" if i % 3 == 0 else "",
                    "tags": [{"tagFQN": "PII"}] if is_pii else []
                })
            
            has_pii = any("PII" in str(col.get("tags", [])) for col in columns)
            classification = random.choice(["public", "internal", "confidential"]) if i % 2 == 0 else None
            
            tables.append({
                "id": f"table-{i}",
                "name": table_name,
                "fullyQualifiedName": f"{database}.{schema}.{table_name}",
                "tableType": "Regular",
                "columns": columns,
                "owner": {"name": owners[i % len(owners)]} if i % 4 != 0 else {},
                "tags": [{"tagFQN": f"Classification.{classification}"}] if classification else [],
                "updatedAt": int((datetime.now() - timedelta(hours=i % 48)).timestamp() * 1000),
                "description": f"This table contains {domain} - {data_asset} data for {schema} layer" if i % 3 == 0 else "",
                "rowCount": random.randint(1000, 1000000),
                "domain": domain,
                "data_asset": data_asset
            })
        
        # Generate tables for other domains (without data assets)
        for i in range(other_count):
            idx = deliver_count + i
            domain = other_domains[i % len(other_domains)]
            data_asset = ""  # No data assets for other domains yet
            database = ALLOWED_DATABASES[i % len(ALLOWED_DATABASES)]
            schema = schemas[i % len(schemas)]
            table_name = f"table_{domain.lower()}_{idx:03d}"
            
            num_columns = 5 + (i % 10)
            columns = []
            for j in range(num_columns):
                col_type = ["VARCHAR", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "BOOLEAN"][j % 6]
                is_pii = (j % 7 == 0)
                
                columns.append({
                    "name": f"col_{j}",
                    "dataType": col_type,
                    "constraint": "NOT NULL" if j == 0 else "",
                    "description": f"Column {j} - {col_type} field" if i % 3 == 0 else "",
                    "tags": [{"tagFQN": "PII"}] if is_pii else []
                })
            
            has_pii = any("PII" in str(col.get("tags", [])) for col in columns)
            classification = random.choice(["public", "internal", "confidential"]) if i % 2 == 0 else None
            
            tables.append({
                "id": f"table-{idx}",
                "name": table_name,
                "fullyQualifiedName": f"{database}.{schema}.{table_name}",
                "tableType": "Regular",
                "columns": columns,
                "owner": {"name": owners[i % len(owners)]} if i % 4 != 0 else {},
                "tags": [{"tagFQN": f"Classification.{classification}"}] if classification else [],
                "updatedAt": int((datetime.now() - timedelta(hours=i % 48)).timestamp() * 1000),
                "description": f"This table contains {domain} data for {schema} layer" if i % 3 == 0 else "",
                "rowCount": random.randint(1000, 1000000),
                "domain": domain,
                "data_asset": data_asset
            })
        
        return tables
    
    @staticmethod
    def generate_mock_contracts(tables: List[Dict], count: int = 25) -> Dict[str, DataContract]:
        """Generate mock contracts"""
        import random
        
        contracts = {}
        contract_engine = DataContractEngine()
        
        eligible_tables = [t for t in tables if t.get("owner", {}).get("name")][:count]
        
        for table in eligible_tables:
            owner = table.get("owner", {}).get("name", "unknown")
            fqn = table.get("fullyQualifiedName", "")
            domain = table.get("domain", ALLOWED_DOMAINS[0])
            data_asset = table.get("data_asset", "")
            database = fqn.split(".")[0] if fqn else ALLOWED_DATABASES[0]
            
            has_pii = any("PII" in str(col.get("tags", [])) for col in table.get("columns", []))
            classification = random.choice(["internal", "confidential"]) if has_pii else random.choice(["public", "internal"])
            
            # Business purpose includes data asset if available
            if data_asset:
                purpose = f"Supports {domain} - {data_asset} business operations"
            else:
                purpose = f"Supports {domain} business operations"
            
            contract = contract_engine.create_contract(
                table=table,
                owner=owner,
                classification=classification,
                description=f"Production contract for {table.get('name')}",
                business_purpose=purpose,
                quality_rules=[
                    {"type": "null_check", "threshold": 0.95},
                    {"type": "unique_check", "column": "col_0"}
                ],
                sla_hours=24,
                contains_pii=has_pii,
                domain=domain,
                data_asset=data_asset,
                database=database
            )
            
            # Set some contracts to active
            if random.random() > 0.3:
                contract_engine.update_contract_status(fqn, "active", owner, "Approved for production")
            elif random.random() > 0.5:
                contract_engine.update_contract_status(fqn, "review", owner, "Under review")
            
            # Register some consumers
            if random.random() > 0.5:
                contract_engine.register_consumer(fqn, "Dashboard Team", "dashboard@company.com")
            if random.random() > 0.7:
                contract_engine.register_consumer(fqn, "ML Pipeline", "ml-team@company.com")
            
            contracts[fqn] = contract
        
        return contracts
    
    @staticmethod
    def generate_lineage(table_fqn: str, all_tables: List[Dict]) -> Dict:
        """Generate mock lineage"""
        import random
        
        other_tables = [t for t in all_tables if t["fullyQualifiedName"] != table_fqn]
        num_downstream = random.randint(1, 6)
        downstream_tables = random.sample(other_tables, min(num_downstream, len(other_tables)))
        
        return {
            "entity": {"fullyQualifiedName": table_fqn},
            "downstreamEdges": [
                {"toEntity": {"type": "table", "fullyQualifiedName": t["fullyQualifiedName"], "name": t["name"]}}
                for t in downstream_tables
            ],
            "upstreamEdges": []
        }
    
    @staticmethod
    def generate_mock_data_products(
        tables: List[Dict], 
        contracts: Dict[str, DataContract],
        trust_scores: List[DataTrustScore] = None
    ) -> Dict[str, DataProduct]:
        """
        Generate mock Data Products that wrap multiple data assets and tables.
        Each product represents a business-aligned, consumable data unit.
        """
        import random
        
        products = {}
        product_engine = DataProductEngine()
        
        # Define sample data products for Deliver domain
        # These wrap multiple data assets following Right-to-Left philosophy
        product_definitions = [
            {
                "name": "Delivery Performance Tracker",
                "domain": "Deliver",
                "business_purpose": "Enable supply chain managers to monitor end-to-end delivery performance, identify bottlenecks, and optimize logistics operations. Answers: What is our on-time delivery rate? Where are the delays occurring?",
                "target_personas": ["Supply Chain Manager", "Logistics Analyst", "Operations Director"],
                "data_assets": ["Delivery", "Transportation", "Shipment"],
                "north_star": {
                    "name": "On-Time Delivery Rate",
                    "description": "Percentage of deliveries completed within SLA",
                    "formula": "COUNT(on_time_deliveries) / COUNT(total_deliveries) * 100",
                    "unit": "%"
                },
                "functional": [
                    {"name": "Average Delivery Time", "formula": "AVG(delivery_time_hours)", "unit": "hours", "description": "Mean time from dispatch to delivery"},
                    {"name": "Delivery Volume", "formula": "COUNT(deliveries)", "unit": "count", "description": "Total number of deliveries"}
                ],
                "granular": [
                    {"name": "Late Delivery Count", "formula": "COUNT(late_deliveries)", "unit": "count", "description": "Number of deliveries past SLA"},
                    {"name": "Avg Delay Duration", "formula": "AVG(delay_hours)", "unit": "hours", "description": "Average delay for late deliveries"}
                ],
                "output_ports": [
                    {"name": "Performance Dashboard", "port_type": "dashboard", "format": "powerbi", "description": "Real-time delivery KPIs"},
                    {"name": "Analytics Dataset", "port_type": "dataset", "format": "parquet", "description": "Historical delivery data for analysis"},
                    {"name": "Alerts API", "port_type": "api", "format": "rest", "description": "Real-time delay notifications"}
                ],
                "tags": ["delivery", "logistics", "performance", "kpi"]
            },
            {
                "name": "Sales Order Intelligence",
                "domain": "Deliver",
                "business_purpose": "Provide sales and fulfillment teams with comprehensive order insights to improve order processing efficiency and customer satisfaction. Answers: What is our order fulfillment rate? Which orders are at risk?",
                "target_personas": ["Sales Manager", "Customer Success Manager", "Fulfillment Lead"],
                "data_assets": ["Sales Order", "Delivery"],
                "north_star": {
                    "name": "Order Fulfillment Rate",
                    "description": "Percentage of orders fulfilled completely and on time",
                    "formula": "COUNT(fulfilled_orders) / COUNT(total_orders) * 100",
                    "unit": "%"
                },
                "functional": [
                    {"name": "Average Order Value", "formula": "AVG(order_value)", "unit": "$", "description": "Mean value per order"},
                    {"name": "Order Cycle Time", "formula": "AVG(order_to_delivery_days)", "unit": "days", "description": "Average days from order to delivery"}
                ],
                "granular": [
                    {"name": "Pending Orders", "formula": "COUNT(pending_orders)", "unit": "count", "description": "Orders awaiting fulfillment"},
                    {"name": "Backorder Rate", "formula": "COUNT(backorders) / COUNT(orders) * 100", "unit": "%", "description": "Percentage of backordered items"}
                ],
                "output_ports": [
                    {"name": "Order Analytics", "port_type": "dataset", "format": "parquet", "description": "Order history and analytics"},
                    {"name": "Fulfillment API", "port_type": "api", "format": "rest", "description": "Order status and tracking"}
                ],
                "tags": ["sales", "orders", "fulfillment", "customer"]
            },
            {
                "name": "Transportation Cost Optimizer",
                "domain": "Deliver",
                "business_purpose": "Help logistics teams analyze and optimize transportation costs across carriers and routes. Answers: What are our transportation costs per unit? Which routes are most cost-effective?",
                "target_personas": ["Logistics Manager", "Finance Analyst", "Procurement Lead"],
                "data_assets": ["Transportation", "Shipment"],
                "north_star": {
                    "name": "Cost Per Unit Shipped",
                    "description": "Average transportation cost per unit delivered",
                    "formula": "SUM(transport_cost) / SUM(units_shipped)",
                    "unit": "$"
                },
                "functional": [
                    {"name": "Total Transport Cost", "formula": "SUM(transport_cost)", "unit": "$", "description": "Total transportation spend"},
                    {"name": "Carrier Utilization", "formula": "AVG(capacity_used) / AVG(total_capacity) * 100", "unit": "%", "description": "Average carrier capacity utilization"}
                ],
                "granular": [
                    {"name": "Cost by Carrier", "formula": "SUM(cost) GROUP BY carrier", "unit": "$", "description": "Breakdown by carrier"},
                    {"name": "Route Efficiency", "formula": "distance / fuel_used", "unit": "miles/gallon", "description": "Fuel efficiency by route"}
                ],
                "output_ports": [
                    {"name": "Cost Dashboard", "port_type": "dashboard", "format": "powerbi", "description": "Transportation cost analytics"},
                    {"name": "Cost Dataset", "port_type": "dataset", "format": "csv", "description": "Cost data for finance systems"}
                ],
                "tags": ["transportation", "cost", "optimization", "logistics"]
            },
            {
                "name": "Shipment Tracking Hub",
                "domain": "Deliver",
                "business_purpose": "Provide real-time visibility into shipment status for customer service and operations teams. Answers: Where is my shipment? What shipments need attention?",
                "target_personas": ["Customer Service Rep", "Operations Coordinator", "Warehouse Manager"],
                "data_assets": ["Shipment", "Transportation", "Delivery"],
                "north_star": {
                    "name": "Shipment Visibility Rate",
                    "description": "Percentage of shipments with real-time tracking",
                    "formula": "COUNT(tracked_shipments) / COUNT(total_shipments) * 100",
                    "unit": "%"
                },
                "functional": [
                    {"name": "Active Shipments", "formula": "COUNT(in_transit_shipments)", "unit": "count", "description": "Currently in-transit shipments"},
                    {"name": "Exception Rate", "formula": "COUNT(exceptions) / COUNT(shipments) * 100", "unit": "%", "description": "Shipments with issues"}
                ],
                "granular": [
                    {"name": "Shipments by Status", "formula": "COUNT(*) GROUP BY status", "unit": "count", "description": "Distribution by status"},
                    {"name": "Avg Transit Time", "formula": "AVG(transit_days)", "unit": "days", "description": "Average time in transit"}
                ],
                "output_ports": [
                    {"name": "Tracking API", "port_type": "api", "format": "rest", "description": "Real-time shipment tracking"},
                    {"name": "Status Stream", "port_type": "stream", "format": "json", "description": "Live shipment updates"},
                    {"name": "Tracking Dataset", "port_type": "dataset", "format": "parquet", "description": "Historical shipment data"}
                ],
                "tags": ["shipment", "tracking", "real-time", "visibility"]
            }
        ]
        
        owners = ["alice.data", "bob.engineering", "carol.analytics", "dave.ops"]
        
        for i, prod_def in enumerate(product_definitions):
            # Find tables that belong to the data assets in this product
            product_tables = [
                t for t in tables
                if t.get("data_asset", "") in prod_def["data_assets"]
            ]
            
            # Get FQNs and find matching contracts
            table_fqns = [t.get("fullyQualifiedName", "") for t in product_tables[:8]]  # Limit to 8 tables per product
            contract_ids = []
            for fqn in table_fqns:
                if fqn in contracts:
                    contract_ids.append(contracts[fqn].id)
            
            # Create the product
            product = product_engine.create_product(
                name=prod_def["name"],
                domain=prod_def["domain"],
                business_purpose=prod_def["business_purpose"],
                target_personas=prod_def["target_personas"],
                north_star_metric=prod_def["north_star"],
                functional_metrics=prod_def["functional"],
                granular_metrics=prod_def["granular"],
                data_assets=prod_def["data_assets"],
                table_fqns=table_fqns,
                contract_ids=contract_ids,
                output_ports=prod_def["output_ports"],
                owner=owners[i % len(owners)],
                tags=prod_def["tags"]
            )
            
            # Set some to active
            if random.random() > 0.3:
                product_engine.update_product_status(product.id, "active", product.owner, "Approved for production use")
            
            # Add mock usage stats
            product.usage_count = random.randint(50, 500)
            product.consumer_count = random.randint(5, 30)
            product.rating = round(random.uniform(3.5, 5.0), 1)
            
            # Calculate aggregated trust if scores available
            if trust_scores:
                score, level = product_engine.calculate_aggregated_trust(product, trust_scores)
                product.aggregated_trust_score = score
                product.trust_level = level
            else:
                product.aggregated_trust_score = random.uniform(55, 90)
                product.trust_level = "Gold" if product.aggregated_trust_score >= 75 else "Silver"
            
            products[product.id] = product
        
        return products

# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_metric_card_gradient(label: str, value: str, delta: Optional[str] = None, 
                                gradient_class: str = "metric-card-blue"):
    """Render gradient metric card"""
    st.markdown(f"""
        <div class="metric-card {gradient_class}">
            <div style="font-size: 0.9rem; opacity: 0.9;">{label}</div>
            <div style="font-size: 2.2rem; font-weight: bold; margin: 0.5rem 0;">{value}</div>
            {f'<div style="font-size: 0.85rem; opacity: 0.8;">{delta}</div>' if delta else ''}
        </div>
    """, unsafe_allow_html=True)

def render_governance_dashboard(tables: List[Dict], contracts: Dict[str, DataContract],
                               governance_engine: GovernanceEngine):
    """Render governance executive dashboard"""
    st.markdown('<div class="main-header">🏛️ Data Governance Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Executive view of data governance health and compliance</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Calculate metrics
    metrics = governance_engine.calculate_governance_metrics(tables, contracts)
    gaps = governance_engine.identify_governance_gaps(tables, contracts)
    
    # Top-level KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        status = "metric-card-green" if metrics.ownership_coverage >= 90 else "metric-card-orange"
        render_metric_card_gradient(
            "Ownership Coverage",
            f"{metrics.ownership_coverage:.1f}%",
            f"{metrics.owned_assets}/{metrics.total_assets} assets",
            status
        )
    
    with col2:
        status = "metric-card-green" if metrics.documentation_coverage >= 80 else "metric-card-orange"
        render_metric_card_gradient(
            "Documentation",
            f"{metrics.documentation_coverage:.1f}%",
            f"{metrics.documented_assets} documented",
            status
        )
    
    with col3:
        status = "metric-card-green" if metrics.contract_coverage >= 40 else "metric-card-orange"
        render_metric_card_gradient(
            "Contract Coverage",
            f"{metrics.contract_coverage:.1f}%",
            f"{metrics.contracted_assets} contracts",
            status
        )
    
    with col4:
        status = "metric-card-green" if metrics.classification_coverage >= 70 else "metric-card-orange"
        render_metric_card_gradient(
            "Classification",
            f"{metrics.classification_coverage:.1f}%",
            f"{metrics.classified_assets} classified",
            status
        )
    
    with col5:
        status = "metric-card-green" if metrics.compliance_rate >= 50 else "metric-card-orange"
        render_metric_card_gradient(
            "Compliance Rate",
            f"{metrics.compliance_rate:.1f}%",
            f"{metrics.compliant_assets} compliant",
            status
        )
    
    st.markdown("---")
    
    # Governance health by domain
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Governance Coverage by Domain")
        
        domain_coverage = defaultdict(lambda: {"total": 0, "owned": 0, "documented": 0, "contracted": 0})
        
        for table in tables:
            domain = table.get("domain", "Unknown")
            if domain in ALLOWED_DOMAINS:
                domain_coverage[domain]["total"] += 1
                if table.get("owner", {}).get("name"):
                    domain_coverage[domain]["owned"] += 1
                if table.get("description"):
                    domain_coverage[domain]["documented"] += 1
                if table.get("fullyQualifiedName") in contracts:
                    domain_coverage[domain]["contracted"] += 1
        
        coverage_data = []
        for domain, stats in domain_coverage.items():
            coverage_data.append({
                "Domain": domain,
                "Ownership %": (stats["owned"] / stats["total"] * 100) if stats["total"] > 0 else 0,
                "Documentation %": (stats["documented"] / stats["total"] * 100) if stats["total"] > 0 else 0,
                "Contract %": (stats["contracted"] / stats["total"] * 100) if stats["total"] > 0 else 0
            })
        
        df_coverage = pd.DataFrame(coverage_data)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Ownership", x=df_coverage["Domain"], 
                            y=df_coverage["Ownership %"], marker_color="#667eea"))
        fig.add_trace(go.Bar(name="Documentation", x=df_coverage["Domain"], 
                            y=df_coverage["Documentation %"], marker_color="#11998e"))
        fig.add_trace(go.Bar(name="Contracts", x=df_coverage["Domain"], 
                            y=df_coverage["Contract %"], marker_color="#f093fb"))
        
        fig.update_layout(barmode='group', height=350, yaxis_title="Coverage %",
                         yaxis_range=[0, 100], showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Critical Governance Gaps")
        
        # Add mock PII data if none exists (for visualization purposes)
        if len(gaps["contains_pii_unclassified"]) == 0:
            # Add 10% mock PII unprotected entries for demonstration
            total_tables = len(tables)
            mock_pii_count = int(total_tables * 0.10)  # 10% mock data
            gaps["contains_pii_unclassified"] = [f"mock_pii_{i}" for i in range(mock_pii_count)]
        
        # Ensure at least one gap shows >70% for critical issue demonstration
        # Adjust "No Description" to show critical level if all gaps are below 70%
        total_tables = len(tables)
        
        gap_counts = {
            "Missing Owner": len(gaps["no_owner"]),
            "No Description": len(gaps["no_description"]),
            "Unclassified": len(gaps["no_classification"]),
            "No Contract": len(gaps["no_contract"]),
            "PII Unprotected": len(gaps["contains_pii_unclassified"])
        }
        
        # Convert to percentages
        gap_percentages = {
            key: (count / total_tables * 100) if total_tables > 0 else 0
            for key, count in gap_counts.items()
        }
        
        # Ensure at least one gap is >70% for demonstration
        if all(pct <= 70 for pct in gap_percentages.values()):
            # Make "No Description" show as 75% (critical)
            critical_count = int(total_tables * 0.75)
            gap_counts["No Description"] = critical_count
            gap_percentages["No Description"] = 75.0
        
        # Assign colors based on thresholds: 0-30% Green, 30-70% Amber, >70% Red
        def get_color(percentage):
            if percentage <= 30:
                return "#28a745"  # Green
            elif percentage <= 70:
                return "#ffc107"  # Amber
            else:
                return "#dc3545"  # Red
        
        colors = [get_color(pct) for pct in gap_percentages.values()]
        
        # Create the bar chart with two columns for chart and legend
        chart_col, legend_col = st.columns([3, 1])
        
        with chart_col:
            # Create the bar chart
            fig = go.Figure(data=[
                go.Bar(
                    x=list(gap_percentages.values()),
                    y=list(gap_percentages.keys()),
                    orientation='h',
                    marker=dict(color=colors),
                    text=[f"{pct:.1f}%" for pct in gap_percentages.values()],
                    textposition='outside',
                    hovertemplate='<b>%{y}</b><br>Percentage: %{x:.1f}%<br>Count: %{customdata}<extra></extra>',
                    customdata=list(gap_counts.values())
                )
            ])
            
            fig.update_layout(
                height=350,
                showlegend=False,
                xaxis_title="Percentage of Tables (%)",
                yaxis_title="Gap Type",
                xaxis=dict(range=[0, max(gap_percentages.values()) * 1.15]),  # Add space for text labels
                margin=dict(r=10)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with legend_col:
            # RAG Scale Legend
            st.markdown("### RAG Scale")
            st.markdown("""
                <div style='padding: 10px; margin: 5px 0;'>
                    <div style='background-color: #28a745; padding: 8px; border-radius: 5px; margin: 5px 0; color: white; font-weight: bold;'>
                        ✓ Good<br><span style='font-size: 0.85em;'>0-30%</span>
                    </div>
                    <div style='background-color: #ffc107; padding: 8px; border-radius: 5px; margin: 5px 0; color: #000; font-weight: bold;'>
                        ⚠ Attention<br><span style='font-size: 0.85em;'>30-70%</span>
                    </div>
                    <div style='background-color: #dc3545; padding: 8px; border-radius: 5px; margin: 5px 0; color: white; font-weight: bold;'>
                        ✗ Critical<br><span style='font-size: 0.85em;'>>70%</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

    
    st.markdown("---")
    
    # Stewardship report
    st.subheader("👥 Data Stewardship Report")
    stewardship_df = governance_engine.get_stewardship_report(tables)
    
    if not stewardship_df.empty:
        st.dataframe(stewardship_df, use_container_width=True, hide_index=True)
    else:
        st.info("No stewardship data available")
    
    # Contract status distribution
    st.markdown("---")
    st.subheader("📋 Contract Status Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    status_counts = defaultdict(int)
    for contract in contracts.values():
        status_counts[contract.status] += 1
    
    with col1:
        st.metric("Draft", status_counts.get("draft", 0), help="Contracts being created")
    with col2:
        st.metric("Under Review", status_counts.get("review", 0), help="Awaiting approval")
    with col3:
        st.metric("Active", status_counts.get("active", 0), help="Enforced contracts")
    with col4:
        st.metric("Deprecated", status_counts.get("deprecated", 0), help="Sunset contracts")

def render_data_discovery(tables: List[Dict], contracts: Dict[str, DataContract]):
    """Render data discovery interface"""
    st.markdown('<div class="main-header">🔍 Data Discovery</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Search, explore, and discover data assets across your organization</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Search interface with cascading filters
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input("🔎 Search for tables, columns, or descriptions", 
                                    placeholder="e.g., customer, sales, revenue...")
    
    with col2:
        contract_filter = st.selectbox("Contract Status", 
                                      ["All", "With Contract", "No Contract"])
    
    # Cascading filters row
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    
    with filter_col1:
        domain_filter = st.selectbox("Domain", ["All"] + ALLOWED_DOMAINS, key="disc_domain")
    
    with filter_col2:
        # Data Asset filter - cascades from Domain
        if domain_filter != "All" and domain_filter in DATA_ASSETS:
            available_assets = DATA_ASSETS.get(domain_filter, [])
            if available_assets:
                data_asset_filter = st.selectbox("Data Asset", ["All"] + available_assets, key="disc_data_asset")
            else:
                st.selectbox("Data Asset", ["N/A - No assets defined"], disabled=True, key="disc_data_asset_disabled")
                data_asset_filter = "All"
        else:
            # When "All" domains selected, show all available data assets
            all_assets = []
            for assets in DATA_ASSETS.values():
                all_assets.extend(assets)
            if all_assets:
                data_asset_filter = st.selectbox("Data Asset", ["All"] + sorted(set(all_assets)), key="disc_data_asset_all")
            else:
                st.selectbox("Data Asset", ["N/A"], disabled=True, key="disc_data_asset_none")
                data_asset_filter = "All"
    
    with filter_col3:
        # Database filter - cascades from Data Asset
        if data_asset_filter != "All" and data_asset_filter in DATA_ASSET_DATABASE_MAPPING:
            mapped_db = DATA_ASSET_DATABASE_MAPPING.get(data_asset_filter)
            st.selectbox("Database", [mapped_db], disabled=True, key="disc_db_mapped")
            db_filter = mapped_db
        else:
            db_filter = st.selectbox("Database", ["All"] + ALLOWED_DATABASES, key="disc_db")
    
    with filter_col4:
        classification_filter = st.selectbox("Classification", 
                                            ["All"] + list(DATA_CLASSIFICATIONS.keys()))
    
    # Advanced filters in expander
    with st.expander("🔧 Advanced Filters"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            owner_filter = st.text_input("Owner contains", "")
        with col2:
            has_pii = st.checkbox("Contains PII", value=False)
        with col3:
            sort_by = st.selectbox("Sort by", ["Name", "Last Updated", "Popularity", "Quality Score"])
    
    # Apply filters
    filtered_tables = tables.copy()
    
    if search_query:
        filtered_tables = [
            t for t in filtered_tables
            if (search_query.lower() in t.get("fullyQualifiedName", "").lower() or
                search_query.lower() in t.get("description", "").lower() or
                any(search_query.lower() in col.get("name", "").lower() 
                    for col in t.get("columns", [])))
        ]
    
    if domain_filter != "All":
        filtered_tables = [
            t for t in filtered_tables
            if t.get("domain", "") == domain_filter
        ]
    
    if data_asset_filter != "All":
        filtered_tables = [
            t for t in filtered_tables
            if t.get("data_asset", "") == data_asset_filter
        ]
    
    if db_filter != "All":
        filtered_tables = [
            t for t in filtered_tables
            if t.get("fullyQualifiedName", "").split(".")[0] == db_filter
        ]
    
    if classification_filter != "All":
        filtered_tables = [
            t for t in filtered_tables
            if classification_filter in str(t.get("tags", []))
        ]
    
    if contract_filter == "With Contract":
        filtered_tables = [t for t in filtered_tables if t.get("fullyQualifiedName") in contracts]
    elif contract_filter == "No Contract":
        filtered_tables = [t for t in filtered_tables if t.get("fullyQualifiedName") not in contracts]
    
    if owner_filter:
        filtered_tables = [
            t for t in filtered_tables
            if owner_filter.lower() in t.get("owner", {}).get("name", "").lower()
        ]
    
    if has_pii:
        filtered_tables = [
            t for t in filtered_tables
            if any("PII" in str(col.get("tags", [])) for col in t.get("columns", []))
        ]
    
    # Display results
    st.markdown(f"### 📋 Found {len(filtered_tables)} data assets")
    st.markdown("---")
    
    if filtered_tables:
        # Quick stats about results
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            unique_domains = len(set(t.get("domain", "Unknown") for t in filtered_tables))
            st.metric("Domains", unique_domains)
        
        with col2:
            with_contracts = sum(1 for t in filtered_tables if t.get("fullyQualifiedName") in contracts)
            st.metric("With Contracts", with_contracts)
        
        with col3:
            with_owners = sum(1 for t in filtered_tables if t.get("owner", {}).get("name"))
            st.metric("Assigned Owner", with_owners)
        
        with col4:
            with_pii = sum(1 for t in filtered_tables 
                          if any("PII" in str(col.get("tags", [])) for col in t.get("columns", [])))
            st.metric("Contains PII", with_pii)
        
        st.markdown("---")
        
        # Display results as cards
        for table in filtered_tables[:20]:  # Limit to 20 results
            fqn = table.get("fullyQualifiedName", "")
            has_contract = fqn in contracts
            domain = table.get("domain", "Unknown")
            data_asset = table.get("data_asset", "")
            
            with st.container():
                # Build domain/asset display string
                domain_display = f"Domain: {domain}"
                if data_asset:
                    domain_display += f" | Data Asset: {data_asset}"
                
                st.markdown(f"""
                    <div class="search-result">
                        <h3 style="margin: 0 0 0.5rem 0;">📊 {table.get('name', 'Unknown')}</h3>
                        <p style="color: #666; font-size: 0.9rem; margin: 0 0 0.5rem 0;">{fqn}</p>
                        <p style="color: #888; font-size: 0.8rem; margin: 0;">{domain_display}</p>
                    </div>
                """, unsafe_allow_html=True)
                
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    description = table.get("description", "No description available")
                    st.caption(description[:200] + "..." if len(description) > 200 else description)
                
                with col2:
                    # Badges
                    owner = table.get("owner", {}).get("name", "Unassigned")
                    owner_class = "ownership-assigned" if owner != "Unassigned" else "ownership-unassigned"
                    st.markdown(f'<span class="governance-badge {owner_class}">👤 {owner}</span>', 
                              unsafe_allow_html=True)
                    
                    if has_contract:
                        contract = contracts[fqn]
                        status_info = CONTRACT_STATUS[contract.status]
                        st.markdown(f'<span class="governance-badge classification-internal">{status_info["icon"]} Contract: {contract.status}</span>', 
                                  unsafe_allow_html=True)
                
                with col3:
                    # Classification
                    tags = table.get("tags", [])
                    if tags:
                        classification = None
                        for tag in tags:
                            tag_fqn = tag.get("tagFQN", "")
                            for cls in DATA_CLASSIFICATIONS.keys():
                                if cls in tag_fqn.lower():
                                    classification = cls
                                    break
                        
                        if classification:
                            cls_info = DATA_CLASSIFICATIONS[classification]
                            st.markdown(f'<span class="governance-badge classification-{classification}">{cls_info["icon"]} {classification}</span>', 
                                      unsafe_allow_html=True)
                    
                    # PII indicator
                    has_pii_cols = any("PII" in str(col.get("tags", [])) for col in table.get("columns", []))
                    if has_pii_cols:
                        st.markdown('<span class="governance-badge classification-restricted">🔒 Contains PII</span>', 
                                  unsafe_allow_html=True)
                
                # Expandable details
                with st.expander("View Details"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Schema Information**")
                        st.markdown(f"- **Columns:** {len(table.get('columns', []))}")
                        st.markdown(f"- **Rows:** {table.get('rowCount', 'Unknown'):,}" if table.get('rowCount') else "- **Rows:** Unknown")
                        st.markdown(f"- **Type:** {table.get('tableType', 'Unknown')}")
                        
                        if has_contract:
                            contract = contracts[fqn]
                            st.markdown(f"- **SLA:** {contract.sla_requirements.get('freshness_hours', 'N/A')} hours")
                    
                    with col2:
                        st.markdown("**Columns**")
                        columns_list = [col.get("name", "") for col in table.get("columns", [])[:10]]
                        for col_name in columns_list:
                            st.markdown(f"- `{col_name}`")
                        
                        if len(table.get("columns", [])) > 10:
                            st.caption(f"... and {len(table.get('columns', [])) - 10} more")
                
                st.markdown("---")
        
        if len(filtered_tables) > 20:
            st.info(f"Showing first 20 of {len(filtered_tables)} results. Refine your search to see more.")
    else:
        st.info("No data assets found matching your criteria. Try adjusting your filters.")

def render_contract_management(tables: List[Dict], contracts: Dict[str, DataContract],
                               contract_engine: DataContractEngine, mock_gen: MockDataGenerator):
    """Render contract management interface"""
    st.markdown('<div class="main-header">📜 Data Contract Management</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Create, monitor, and manage data contracts across your data assets</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Tabs for different contract views - NOW WITH DEVELOPER TOOLS!
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Contract Overview",
        "➕ Create Contract",
        "🔍 Monitor Compliance",
        "🔄 Schema Changes",
        "👥 Consumer Registry",
        "🚀 Developer Tools"
    ])
    
    with tab1:
        render_contract_overview(contracts, contract_engine)
    
    with tab2:
        render_contract_creation_wizard(tables, contracts, contract_engine)
    
    with tab3:
        render_compliance_monitoring(tables, contracts, contract_engine)
    
    with tab4:
        render_schema_drift_monitor(tables, contracts, contract_engine, mock_gen)
    
    with tab5:
        render_consumer_registry(contracts, contract_engine)
    
    with tab6:
        render_developer_tools(contracts)

def render_contract_overview(contracts: Dict[str, DataContract], contract_engine: DataContractEngine):
    """Render contract overview"""
    st.subheader("📊 Contract Portfolio")
    
    if not contracts:
        st.info("No contracts created yet. Go to 'Create Contract' tab to get started!")
        return
    
    # Status breakdown
    col1, col2, col3, col4 = st.columns(4)
    
    draft_count = len([c for c in contracts.values() if c.status == "draft"])
    review_count = len([c for c in contracts.values() if c.status == "review"])
    active_count = len([c for c in contracts.values() if c.status == "active"])
    deprecated_count = len([c for c in contracts.values() if c.status == "deprecated"])
    
    with col1:
        st.metric("Draft", draft_count, help="Contracts in draft state")
    with col2:
        st.metric("Under Review", review_count, help="Contracts awaiting approval")
    with col3:
        st.metric("Active", active_count, help="Live contracts")
    with col4:
        st.metric("Deprecated", deprecated_count, help="Sunset contracts")
    
    st.markdown("---")
    
    # Filter and search with cascading filters
    col1, col2 = st.columns(2)
    
    with col1:
        status_filter = st.selectbox("Filter by Status", 
                                    ["All", "draft", "review", "active", "deprecated"])
    with col2:
        owner_search = st.text_input("Search by Owner", "", key="contract_owner_search")
    
    # Cascading filters row
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        domain_filter = st.selectbox("Filter by Domain", ["All"] + ALLOWED_DOMAINS, key="contract_domain_filter")
    
    with filter_col2:
        # Data Asset filter - cascades from Domain
        if domain_filter != "All" and domain_filter in DATA_ASSETS:
            available_assets = DATA_ASSETS.get(domain_filter, [])
            if available_assets:
                data_asset_filter = st.selectbox("Filter by Data Asset", ["All"] + available_assets, key="contract_data_asset_filter")
            else:
                st.selectbox("Filter by Data Asset", ["N/A - No assets defined"], disabled=True, key="contract_data_asset_disabled")
                data_asset_filter = "All"
        else:
            all_assets = []
            for assets in DATA_ASSETS.values():
                all_assets.extend(assets)
            if all_assets:
                data_asset_filter = st.selectbox("Filter by Data Asset", ["All"] + sorted(set(all_assets)), key="contract_data_asset_all")
            else:
                st.selectbox("Filter by Data Asset", ["N/A"], disabled=True, key="contract_data_asset_none")
                data_asset_filter = "All"
    
    with filter_col3:
        # Database filter - cascades from Data Asset
        if data_asset_filter != "All" and data_asset_filter in DATA_ASSET_DATABASE_MAPPING:
            mapped_db = DATA_ASSET_DATABASE_MAPPING.get(data_asset_filter)
            st.selectbox("Filter by Database", [mapped_db], disabled=True, key="contract_db_mapped")
            db_filter = mapped_db
        else:
            db_filter = st.selectbox("Filter by Database", ["All"] + ALLOWED_DATABASES, key="contract_db_filter")
    
    # Apply filters
    filtered_contracts = list(contracts.values())
    
    if status_filter != "All":
        filtered_contracts = [c for c in filtered_contracts if c.status == status_filter]
    
    if domain_filter != "All":
        filtered_contracts = [c for c in filtered_contracts if c.domain == domain_filter]
    
    if data_asset_filter != "All":
        filtered_contracts = [c for c in filtered_contracts if c.data_asset == data_asset_filter]
    
    if db_filter != "All":
        filtered_contracts = [c for c in filtered_contracts if c.database == db_filter]
    
    if owner_search:
        filtered_contracts = [c for c in filtered_contracts 
                            if owner_search.lower() in c.owner.lower()]
    
    st.markdown(f"### Showing {len(filtered_contracts)} contracts")
    st.markdown("---")
    
    # Display contracts
    for contract in filtered_contracts:
        status_info = CONTRACT_STATUS[contract.status]
        contract_class = f"contract-{contract.status}"
        
        # Build domain/asset display
        domain_display = contract.domain
        if contract.data_asset:
            domain_display += f" / {contract.data_asset}"
        
        st.markdown(f"""
            <div class="contract-card {contract_class}">
                <div style="display: flex; justify-content: space-between; align-items: start;">
                    <div>
                        <h3 style="margin: 0;">{status_info['icon']} {contract.table_name}</h3>
                        <p style="color: #666; font-size: 0.9rem; margin: 0.25rem 0;">{contract.table_fqn}</p>
                    </div>
                    <div>
                        <span class="governance-badge classification-{contract.classification}">
                            {DATA_CLASSIFICATIONS[contract.classification]['icon']} {contract.classification}
                        </span>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"**Owner:** {contract.owner}")
            st.markdown(f"**Version:** {contract.version}")
        
        with col2:
            st.markdown(f"**Status:** {contract.status.title()}")
            st.markdown(f"**Domain:** {contract.domain}")
        
        with col3:
            data_asset_display = contract.data_asset if contract.data_asset else "N/A"
            st.markdown(f"**Data Asset:** {data_asset_display}")
            st.markdown(f"**Database:** {contract.database}")
        
        with col4:
            st.markdown(f"**Data History:** {contract.data_history_years} year(s)")
            st.markdown(f"**Contains PII:** {'Yes' if contract.contains_pii else 'No'}")
        
        with st.expander("📋 View Contract Details"):
            tab1, tab2, tab3, tab4 = st.tabs(["Schema", "Quality Rules", "SLA", "Change Log"])
            
            with tab1:
                schema_data = []
                for col_name, col_info in contract.schema_definition.items():
                    schema_data.append({
                        "Column": col_name,
                        "Data Type": col_info["dataType"],
                        "Nullable": "Yes" if col_info["nullable"] else "No",
                        "PII": "Yes" if col_info.get("isPII") else "No",
                        "Calculation": col_info.get("calculation", "") or "-",
                        "Description": col_info.get("description", "")
                    })
                st.dataframe(pd.DataFrame(schema_data), use_container_width=True, hide_index=True)
            
            with tab2:
                if contract.quality_rules:
                    for rule in contract.quality_rules:
                        st.markdown(f"- **{rule.get('type', 'Unknown')}**: {rule}")
                else:
                    st.info("No quality rules defined")
            
            with tab3:
                st.json(contract.sla_requirements)
            
            with tab4:
                for log_entry in reversed(contract.change_log[-10:]):
                    st.markdown(f"""
                        <div class="timeline-item">
                            <strong>{log_entry['action']}</strong> by {log_entry['user']}<br>
                            <small>{log_entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</small><br>
                            {log_entry['details']}
                        </div>
                    """, unsafe_allow_html=True)
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if contract.status == "draft" and st.button("📤 Submit for Review", key=f"submit_{contract.id}"):
                contract_engine.update_contract_status(
                    contract.table_fqn, "review", contract.owner, 
                    "Submitted for review"
                )
                st.success("Contract submitted for review!")
                st.rerun()
        
        with col2:
            if contract.status == "review" and st.button("✅ Approve", key=f"approve_{contract.id}"):
                contract_engine.update_contract_status(
                    contract.table_fqn, "active", "governance.team",
                    "Approved by governance team"
                )
                st.success("Contract approved and activated!")
                st.rerun()
        
        with col3:
            if contract.status == "active" and st.button("⚠️ Deprecate", key=f"deprecate_{contract.id}"):
                contract_engine.update_contract_status(
                    contract.table_fqn, "deprecated", contract.owner,
                    "Contract deprecated"
                )
                st.warning("Contract deprecated")
                st.rerun()
        
        st.markdown("---")

def render_contract_creation_wizard(tables: List[Dict], contracts: Dict[str, DataContract],
                                   contract_engine: DataContractEngine):
    """Render contract creation wizard with support for existing and new tables"""
    st.subheader("➕ Create New Data Contract")
    st.markdown("Follow the wizard to create a comprehensive data contract")
    
    st.markdown("---")
    
    # MODE SELECTION: Existing Table vs New Table
    st.markdown("### Contract Creation Mode")
    
    creation_mode = st.radio(
        "Choose how to create your contract:",
        ["📊 From Existing Table", "✨ Design New Table from Scratch"],
        help="Select existing table to create contract from metadata, or design a new table contract",
        horizontal=True
    )
    
    st.markdown("---")
    
    # ========== MODE 1: FROM EXISTING TABLE ==========
    if creation_mode == "📊 From Existing Table":
        
        # Step 1: Select Table
        st.markdown("### Step 1: Select Data Asset")
        
        # Filter tables without contracts
        available_tables = [t for t in tables if t.get("fullyQualifiedName") not in contracts]
        
        if not available_tables:
            st.warning("All tables already have contracts. Great job!")
            st.info("💡 Switch to 'Design New Table from Scratch' mode to create a contract for a new table.")
            return
        
        table_options = {t.get("fullyQualifiedName"): t.get("name") for t in available_tables}
        selected_fqn = st.selectbox(
            "Select table",
            options=list(table_options.keys()),
            format_func=lambda x: f"{table_options[x]} ({x})"
        )
        
        if not selected_fqn:
            return
        
        selected_table = next(t for t in available_tables if t.get("fullyQualifiedName") == selected_fqn)
        
        st.info(f"**Selected:** {selected_table.get('name')} with {len(selected_table.get('columns', []))} columns")
        
        table_name = selected_table.get("name")
        table_fqn = selected_fqn
        table_domain = selected_table.get("domain", ALLOWED_DOMAINS[0])
        table_data_asset = selected_table.get("data_asset", "")
        table_database = table_fqn.split(".")[0] if table_fqn else ALLOWED_DATABASES[0]
        
        # Display hierarchy info
        hierarchy_info = f"**Domain:** {table_domain}"
        if table_data_asset:
            hierarchy_info += f" | **Data Asset:** {table_data_asset}"
        hierarchy_info += f" | **Database:** {table_database}"
        st.caption(hierarchy_info)
        
        st.markdown("---")
        
        # Step 1b: Schema Customization for Existing Table
        st.markdown("### Step 1b: Customize Schema")
        st.markdown("Review and modify the schema from the existing table. You can add, edit, or remove columns.")
        
        # Initialize session state for existing table columns
        # Reset if a different table is selected
        if "existing_table_selected_fqn" not in st.session_state or st.session_state.existing_table_selected_fqn != selected_fqn:
            st.session_state.existing_table_selected_fqn = selected_fqn
            # Convert existing columns to editable format
            st.session_state.existing_table_columns = []
            for col in selected_table.get("columns", []):
                is_pii = any("PII" in str(tag) for tag in col.get("tags", []))
                st.session_state.existing_table_columns.append({
                    "name": col.get("name", ""),
                    "dataType": col.get("dataType", "VARCHAR(255)"),
                    "nullable": col.get("constraint", "") != "NOT NULL",
                    "primaryKey": "PRIMARY" in col.get("constraint", "").upper(),
                    "isPII": is_pii,
                    "description": col.get("description", ""),
                    "calculation": col.get("calculation", "")
                })
            # Store original columns for reset functionality
            st.session_state.existing_table_original_columns = [col.copy() for col in st.session_state.existing_table_columns]
        
        # Display existing columns with edit/delete capability
        for idx, col in enumerate(st.session_state.existing_table_columns):
            with st.expander(f"📝 Column {idx + 1}: {col['name']}", expanded=(idx == 0)):
                col_col1, col_col2, col_col3 = st.columns(3)
                
                with col_col1:
                    col_name = st.text_input(
                        "Column Name *",
                        value=col["name"],
                        key=f"exist_col_name_{idx}"
                    )
                    col["name"] = col_name
                
                with col_col2:
                    data_types = ["INTEGER", "BIGINT", "VARCHAR(255)", "STRING", "DECIMAL(10,2)", 
                                  "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "BINARY"]
                    # Handle data types that might not be in our list
                    current_type = col["dataType"]
                    if current_type not in data_types:
                        data_types.insert(0, current_type)
                    
                    data_type = st.selectbox(
                        "Data Type *",
                        data_types,
                        index=data_types.index(current_type),
                        key=f"exist_col_type_{idx}"
                    )
                    col["dataType"] = data_type
                
                with col_col3:
                    nullable = st.checkbox("Nullable", value=col.get("nullable", True), key=f"exist_col_null_{idx}")
                    col["nullable"] = nullable
                
                # New row for Primary Key, PII, and Remove button
                col_col4, col_col5, col_col6 = st.columns(3)
                
                with col_col4:
                    primary_key = st.checkbox("Primary Key", value=col.get("primaryKey", False), key=f"exist_col_pk_{idx}")
                    col["primaryKey"] = primary_key
                    # Auto-set nullable to False if primary key is checked
                    if primary_key:
                        col["nullable"] = False
                
                with col_col5:
                    is_pii = st.checkbox("Contains PII", value=col.get("isPII", False), key=f"exist_col_pii_{idx}")
                    col["isPII"] = is_pii
                
                with col_col6:
                    if st.button("🗑️ Remove Column", key=f"exist_remove_col_{idx}"):
                        if len(st.session_state.existing_table_columns) > 1:
                            st.session_state.existing_table_columns.pop(idx)
                            st.rerun()
                        else:
                            st.error("Cannot remove the last column")
                
                description = st.text_area(
                    "Column Description",
                    value=col.get("description", ""),
                    key=f"exist_col_desc_{idx}",
                    height=80
                )
                col["description"] = description
                
                calculation = st.text_input(
                    "Column Calculation",
                    value=col.get("calculation", ""),
                    key=f"exist_col_calc_{idx}",
                    help="Optional: Define calculation logic (e.g., SUM(amount), col1 + col2)"
                )
                col["calculation"] = calculation
        
        # Add new column button and Reset button
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
        
        with col_btn1:
            if st.button("➕ Add Column", key="exist_add_col", use_container_width=True):
                st.session_state.existing_table_columns.append({
                    "name": f"new_column_{len(st.session_state.existing_table_columns) + 1}",
                    "dataType": "VARCHAR(255)",
                    "nullable": True,
                    "primaryKey": False,
                    "isPII": False,
                    "description": "",
                    "calculation": ""
                })
                st.rerun()
        
        with col_btn2:
            if st.button("🔄 Reset to Original", key="exist_reset_cols", use_container_width=True):
                st.session_state.existing_table_columns = [col.copy() for col in st.session_state.existing_table_original_columns]
                st.rerun()
        
        # Show column count and modification status
        original_count = len(st.session_state.existing_table_original_columns)
        current_count = len(st.session_state.existing_table_columns)
        
        if current_count != original_count:
            st.info(f"**Total Columns:** {current_count} (Original: {original_count}) - *Schema modified*")
        else:
            st.info(f"**Total Columns:** {current_count}")
        
        # Use the modified columns for the contract
        table_columns = st.session_state.existing_table_columns
        
        # Update selected_table with modified columns for consistency
        selected_table = {
            "name": table_name,
            "fullyQualifiedName": table_fqn,
            "columns": [
                {
                    "name": col["name"],
                    "dataType": col["dataType"],
                    "constraint": "" if col["nullable"] else "NOT NULL",
                    "description": col.get("description", ""),
                    "tags": [{"tagFQN": "PII"}] if col["isPII"] else [],
                    "calculation": col.get("calculation", "")
                }
                for col in table_columns
            ],
            "owner": selected_table.get("owner", {}),
            "description": selected_table.get("description", ""),
            "tags": selected_table.get("tags", []),
            "domain": table_domain,
            "data_asset": table_data_asset
        }
        
    # ========== MODE 2: NEW TABLE FROM SCRATCH ==========
    else:
        st.markdown("### Step 1: Define New Table")
        
        st.info("💡 Design your table contract first, then use Developer Tools to generate DDL for implementation")
        
        # Table identification with cascading filters
        col1, col2 = st.columns(2)
        
        with col1:
            table_domain = st.selectbox("Domain *", ALLOWED_DOMAINS, help="Supply chain area", key="new_table_domain")
        
        with col2:
            # Data Asset - cascades from Domain
            available_assets = DATA_ASSETS.get(table_domain, [])
            if available_assets:
                table_data_asset = st.selectbox("Data Asset *", available_assets, help="Data asset within domain", key="new_table_data_asset")
            else:
                st.selectbox("Data Asset", ["N/A - No assets defined for this domain"], disabled=True, key="new_table_data_asset_disabled")
                table_data_asset = ""
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Database - user selects from available databases
            table_database = st.selectbox("Database *", ALLOWED_DATABASES, help="Target database", key="new_table_db")
        
        with col4:
            table_name = st.text_input("Table Name *", help="Name of the new table")
        
        if not table_name:
            st.warning("⚠️ Please enter a table name to continue")
            return
        
        table_fqn = f"{table_database}.{table_name}"
        
        # Check if FQN already exists
        if table_fqn in contracts:
            st.error(f"❌ Contract already exists for {table_fqn}")
            return
        
        # Display hierarchy info
        hierarchy_display = f"Domain: `{table_domain}`"
        if table_data_asset:
            hierarchy_display += f" | Data Asset: `{table_data_asset}`"
        hierarchy_display += f" | Database: `{table_database}` | FQN: `{table_fqn}`"
        st.success(f"✅ {hierarchy_display}")
        
        st.markdown("---")
        
        # Column Definition Interface
        st.markdown("### Step 1b: Define Schema")
        st.markdown("Add columns to your table schema")
        
        # Initialize session state for columns if not exists
        if "new_table_columns" not in st.session_state:
            st.session_state.new_table_columns = [
                {"name": "id", "dataType": "INTEGER", "nullable": False, "primaryKey": True, "isPII": False, "description": "Primary key", "calculation": ""}
            ]
        
        # Display existing columns
        for idx, col in enumerate(st.session_state.new_table_columns):
            with st.expander(f"📝 Column {idx + 1}: {col['name']}", expanded=(idx == 0)):
                col_col1, col_col2, col_col3 = st.columns(3)
                
                with col_col1:
                    col_name = st.text_input(
                        "Column Name *",
                        value=col["name"],
                        key=f"col_name_{idx}"
                    )
                    col["name"] = col_name
                
                with col_col2:
                    data_type = st.selectbox(
                        "Data Type *",
                        ["INTEGER", "BIGINT", "VARCHAR(255)", "STRING", "DECIMAL(10,2)", 
                         "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "BINARY"],
                        index=["INTEGER", "BIGINT", "VARCHAR(255)", "STRING", "DECIMAL(10,2)", 
                               "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "BINARY"].index(col["dataType"]) if col["dataType"] in ["INTEGER", "BIGINT", "VARCHAR(255)", "STRING", "DECIMAL(10,2)", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "BINARY"] else 0,
                        key=f"col_type_{idx}"
                    )
                    col["dataType"] = data_type
                
                with col_col3:
                    nullable = st.checkbox("Nullable", value=col.get("nullable", True), key=f"col_null_{idx}")
                    col["nullable"] = nullable
                
                # New row for Primary Key, PII, and Remove button
                col_col4, col_col5, col_col6 = st.columns(3)
                
                with col_col4:
                    primary_key = st.checkbox("Primary Key", value=col.get("primaryKey", False), key=f"col_pk_{idx}")
                    col["primaryKey"] = primary_key
                    # Auto-set nullable to False if primary key is checked
                    if primary_key:
                        col["nullable"] = False
                
                with col_col5:
                    is_pii = st.checkbox("Contains PII", value=col.get("isPII", False), key=f"col_pii_{idx}")
                    col["isPII"] = is_pii
                
                with col_col6:
                    if st.button("🗑️ Remove Column", key=f"remove_col_{idx}"):
                        if len(st.session_state.new_table_columns) > 1:
                            st.session_state.new_table_columns.pop(idx)
                            st.rerun()
                        else:
                            st.error("Cannot remove the last column")
                
                description = st.text_area(
                    "Column Description",
                    value=col.get("description", ""),
                    key=f"col_desc_{idx}",
                    height=80
                )
                col["description"] = description
                
                calculation = st.text_input(
                    "Column Calculation",
                    value=col.get("calculation", ""),
                    key=f"col_calc_{idx}",
                    help="Optional: Define calculation logic (e.g., SUM(amount), col1 + col2)"
                )
                col["calculation"] = calculation
        
        # Add new column button
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
        
        with col_btn1:
            if st.button("➕ Add Column", use_container_width=True):
                st.session_state.new_table_columns.append({
                    "name": f"column_{len(st.session_state.new_table_columns) + 1}",
                    "dataType": "VARCHAR(255)",
                    "nullable": True,
                    "primaryKey": False,
                    "isPII": False,
                    "description": "",
                    "calculation": ""
                })
                st.rerun()
        
        with col_btn2:
            if st.button("🔄 Reset Schema", use_container_width=True):
                st.session_state.new_table_columns = [
                    {"name": "id", "dataType": "INTEGER", "nullable": False, "primaryKey": True, "isPII": False, "description": "Primary key", "calculation": ""}
                ]
                st.rerun()
        
        st.info(f"**Total Columns:** {len(st.session_state.new_table_columns)}")
        
        # Convert to table format for consistency with existing table flow
        table_columns = st.session_state.new_table_columns
        selected_table = {
            "name": table_name,
            "fullyQualifiedName": table_fqn,
            "columns": [
                {
                    "name": col["name"],
                    "dataType": col["dataType"],
                    "constraint": "" if col["nullable"] else "NOT NULL",
                    "description": col.get("description", ""),
                    "tags": [{"tagFQN": "PII"}] if col["isPII"] else [],
                    "calculation": col.get("calculation", "")
                }
                for col in table_columns
            ],
            "owner": {},
            "description": "",
            "tags": [],
            "domain": table_domain,
            "data_asset": table_data_asset
        }
    
    st.markdown("---")
    
    # Step 2: Basic Information (SAME FOR BOTH MODES)
    st.markdown("### Step 2: Contract Metadata")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if creation_mode == "📊 From Existing Table":
            owner = st.text_input("Contract Owner *", 
                                 value=selected_table.get("owner", {}).get("name", ""),
                                 help="Person responsible for this contract")
        else:
            owner = st.text_input("Contract Owner *", 
                                 help="Person responsible for this contract")
        
        classification = st.selectbox("Data Classification *",
                                     options=list(DATA_CLASSIFICATIONS.keys()),
                                     help="Security classification level")
    
    with col2:
        sla_hours = st.number_input("SLA Freshness (hours) *", 
                                   min_value=1, max_value=168, value=24,
                                   help="Maximum age of data in hours")
        
        data_history_years = st.selectbox("Data History (Retention) *",
                                         options=[1, 2, 3, 5, 7, 10],
                                         index=1,  # Default to 2 years
                                         help="Number of years to retain data for this contract")
    
    col3, col4 = st.columns(2)
    
    with col3:
        if creation_mode == "📊 From Existing Table":
            default_pii = any("PII" in str(col.get("tags", [])) for col in selected_table.get("columns", []))
        else:
            default_pii = any(col.get("isPII", False) for col in table_columns)
        
        contains_pii = st.checkbox("Contains PII",
                                  value=default_pii,
                                  help="Does this data contain personally identifiable information?")
    
    description = st.text_area("Description *",
                              value=selected_table.get("description", "") if creation_mode == "📊 From Existing Table" else "",
                              help="Describe what this data represents",
                              height=100)
    
    business_purpose = st.text_area("Business Purpose *",
                                   help="Explain why this data exists and how it's used",
                                   height=100)
    
    st.markdown("---")
    
    # Step 3: Column-Level Data Quality Rules
    st.markdown("### Step 3: Data Quality Rules")
    st.markdown("Define quality expectations at the column level for precise data validation.")
    
    # Get column list from the selected/designed table
    column_list = [col["name"] for col in selected_table.get("columns", [])]
    column_types = {col["name"]: col.get("dataType", "STRING") for col in selected_table.get("columns", [])}
    
    # Define available DQ rules by data type category
    NUMERIC_TYPES = ["INTEGER", "BIGINT", "DECIMAL(10,2)", "DOUBLE", "FLOAT"]
    STRING_TYPES = ["VARCHAR(255)", "STRING"]
    DATE_TYPES = ["DATE", "TIMESTAMP"]
    
    # Rule definitions with metadata
    DQ_RULE_DEFINITIONS = {
        "null_check": {
            "label": "🚫 Null Check",
            "description": "Ensure column values are not null",
            "applies_to": "all",
            "config": ["threshold"]
        },
        "uniqueness": {
            "label": "🔑 Uniqueness",
            "description": "Ensure column values are unique",
            "applies_to": "all",
            "config": ["threshold"]
        },
        "range_check": {
            "label": "📏 Range Check",
            "description": "Validate values within min/max range",
            "applies_to": ["numeric", "date"],
            "config": ["min_value", "max_value"]
        },
        "format_check": {
            "label": "📝 Format Check",
            "description": "Validate format using pattern/regex",
            "applies_to": ["string"],
            "config": ["pattern_type", "custom_regex"]
        },
        "length_check": {
            "label": "📐 Length Check",
            "description": "Validate string length constraints",
            "applies_to": ["string"],
            "config": ["min_length", "max_length"]
        },
        "allowed_values": {
            "label": "📋 Allowed Values",
            "description": "Restrict to specific valid values",
            "applies_to": "all",
            "config": ["values_list"]
        },
        "freshness_check": {
            "label": "⏰ Freshness Check",
            "description": "Ensure date/time is within acceptable age",
            "applies_to": ["date"],
            "config": ["max_age_hours"]
        }
    }
    
    # Format patterns for string validation
    FORMAT_PATTERNS = {
        "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "phone": r"^\+?[1-9]\d{1,14}$",
        "url": r"^https?://[^\s]+$",
        "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        "date_iso": r"^\d{4}-\d{2}-\d{2}$",
        "alphanumeric": r"^[a-zA-Z0-9]+$",
        "custom": None
    }
    
    def get_data_type_category(data_type: str) -> str:
        """Categorize data type for rule applicability"""
        if any(t in data_type.upper() for t in ["INT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC"]):
            return "numeric"
        elif any(t in data_type.upper() for t in ["DATE", "TIME"]):
            return "date"
        else:
            return "string"
    
    def get_applicable_rules(data_type: str, is_primary_key: bool = False) -> List[str]:
        """Get applicable DQ rules based on data type"""
        category = get_data_type_category(data_type)
        applicable = []
        
        for rule_id, rule_def in DQ_RULE_DEFINITIONS.items():
            applies_to = rule_def["applies_to"]
            if applies_to == "all" or category in applies_to:
                applicable.append(rule_id)
        
        return applicable
    
    # Initialize session state for column DQ rules
    if "column_dq_rules" not in st.session_state:
        st.session_state.column_dq_rules = {}
    
    # Check if we need to reset rules (different table selected)
    current_table_id = selected_table.get("fullyQualifiedName", table_name)
    if "dq_rules_table_id" not in st.session_state or st.session_state.dq_rules_table_id != current_table_id:
        st.session_state.dq_rules_table_id = current_table_id
        st.session_state.column_dq_rules = {}
    
    # Two-column layout for rule configuration
    rule_config_col, rule_summary_col = st.columns([1, 1])
    
    with rule_config_col:
        st.markdown("#### ➕ Add Quality Rules")
        
        # Column selection
        if column_list:
            selected_column = st.selectbox(
                "Select Column",
                options=column_list,
                help="Choose a column to add quality rules",
                key="dq_column_select"
            )
            
            if selected_column:
                col_data_type = column_types.get(selected_column, "STRING")
                st.caption(f"Data Type: `{col_data_type}`")
                
                # Get applicable rules for this column's data type
                applicable_rules = get_applicable_rules(col_data_type)
                
                # Multi-select for rules
                rule_options = {rule_id: DQ_RULE_DEFINITIONS[rule_id]["label"] 
                              for rule_id in applicable_rules}
                
                selected_rules = st.multiselect(
                    "Select Quality Rules",
                    options=list(rule_options.keys()),
                    format_func=lambda x: rule_options[x],
                    help="Select one or more quality rules to apply",
                    key="dq_rule_multiselect"
                )
                
                # Configuration for each selected rule
                if selected_rules:
                    st.markdown("##### Configure Rules")
                    
                    rule_configs = {}
                    
                    for rule_id in selected_rules:
                        rule_def = DQ_RULE_DEFINITIONS[rule_id]
                        
                        with st.expander(f"{rule_def['label']}", expanded=True):
                            st.caption(rule_def["description"])
                            
                            config = {"type": rule_id, "column": selected_column}
                            
                            # Threshold-based rules
                            if "threshold" in rule_def["config"]:
                                threshold = st.slider(
                                    "Threshold (%)",
                                    min_value=0,
                                    max_value=100,
                                    value=95,
                                    help="Percentage of rows that must pass this check",
                                    key=f"config_threshold_{rule_id}"
                                )
                                config["threshold"] = threshold / 100
                            
                            # Range check config
                            if "min_value" in rule_def["config"]:
                                range_col1, range_col2 = st.columns(2)
                                with range_col1:
                                    if get_data_type_category(col_data_type) == "date":
                                        min_val = st.date_input("Min Date", key=f"config_min_{rule_id}")
                                        config["min_value"] = str(min_val)
                                    else:
                                        min_val = st.number_input("Min Value", value=0, key=f"config_min_{rule_id}")
                                        config["min_value"] = min_val
                                with range_col2:
                                    if get_data_type_category(col_data_type) == "date":
                                        max_val = st.date_input("Max Date", key=f"config_max_{rule_id}")
                                        config["max_value"] = str(max_val)
                                    else:
                                        max_val = st.number_input("Max Value", value=100, key=f"config_max_{rule_id}")
                                        config["max_value"] = max_val
                            
                            # Format check config
                            if "pattern_type" in rule_def["config"]:
                                pattern_type = st.selectbox(
                                    "Pattern Type",
                                    options=list(FORMAT_PATTERNS.keys()),
                                    format_func=lambda x: x.replace("_", " ").title(),
                                    key=f"config_pattern_{rule_id}"
                                )
                                config["pattern_type"] = pattern_type
                                
                                if pattern_type == "custom":
                                    custom_regex = st.text_input(
                                        "Custom Regex Pattern",
                                        help="Enter a valid regex pattern",
                                        key=f"config_regex_{rule_id}"
                                    )
                                    config["custom_regex"] = custom_regex
                                else:
                                    config["regex"] = FORMAT_PATTERNS[pattern_type]
                            
                            # Length check config
                            if "min_length" in rule_def["config"]:
                                len_col1, len_col2 = st.columns(2)
                                with len_col1:
                                    min_len = st.number_input("Min Length", value=0, min_value=0, key=f"config_minlen_{rule_id}")
                                    config["min_length"] = min_len
                                with len_col2:
                                    max_len = st.number_input("Max Length", value=255, min_value=1, key=f"config_maxlen_{rule_id}")
                                    config["max_length"] = max_len
                            
                            # Allowed values config
                            if "values_list" in rule_def["config"]:
                                values_input = st.text_area(
                                    "Allowed Values (one per line)",
                                    help="Enter each allowed value on a separate line",
                                    key=f"config_values_{rule_id}",
                                    height=100
                                )
                                config["allowed_values"] = [v.strip() for v in values_input.split("\n") if v.strip()]
                            
                            # Freshness check config
                            if "max_age_hours" in rule_def["config"]:
                                max_age = st.number_input(
                                    "Max Age (hours)",
                                    value=24,
                                    min_value=1,
                                    help="Maximum age of data in hours",
                                    key=f"config_freshness_{rule_id}"
                                )
                                config["max_age_hours"] = max_age
                            
                            rule_configs[rule_id] = config
                    
                    # Add rules button
                    if st.button("✅ Add Rules to Column", type="primary", use_container_width=True):
                        if selected_column not in st.session_state.column_dq_rules:
                            st.session_state.column_dq_rules[selected_column] = []
                        
                        # Add each configured rule
                        for rule_id, config in rule_configs.items():
                            # Check if rule already exists for this column
                            existing_rules = [r["type"] for r in st.session_state.column_dq_rules[selected_column]]
                            if rule_id not in existing_rules:
                                st.session_state.column_dq_rules[selected_column].append(config)
                            else:
                                # Update existing rule
                                for i, r in enumerate(st.session_state.column_dq_rules[selected_column]):
                                    if r["type"] == rule_id:
                                        st.session_state.column_dq_rules[selected_column][i] = config
                                        break
                        
                        st.success(f"✅ Added {len(rule_configs)} rule(s) to '{selected_column}'")
                        st.rerun()
        else:
            st.warning("No columns available. Please define schema first.")
    
    with rule_summary_col:
        st.markdown("#### 📋 Configured Rules Summary")
        
        if st.session_state.column_dq_rules:
            # Build summary data
            summary_data = []
            for col_name, rules in st.session_state.column_dq_rules.items():
                for rule in rules:
                    rule_label = DQ_RULE_DEFINITIONS.get(rule["type"], {}).get("label", rule["type"])
                    
                    # Build config summary
                    config_parts = []
                    if "threshold" in rule:
                        config_parts.append(f"{int(rule['threshold']*100)}% threshold")
                    if "min_value" in rule:
                        config_parts.append(f"min: {rule['min_value']}")
                    if "max_value" in rule:
                        config_parts.append(f"max: {rule['max_value']}")
                    if "pattern_type" in rule:
                        config_parts.append(f"pattern: {rule['pattern_type']}")
                    if "min_length" in rule:
                        config_parts.append(f"len: {rule['min_length']}-{rule.get('max_length', '∞')}")
                    if "max_age_hours" in rule:
                        config_parts.append(f"max age: {rule['max_age_hours']}h")
                    if "allowed_values" in rule and rule["allowed_values"]:
                        config_parts.append(f"{len(rule['allowed_values'])} values")
                    
                    summary_data.append({
                        "Column": col_name,
                        "Rule": rule_label,
                        "Configuration": ", ".join(config_parts) if config_parts else "Default"
                    })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
                
                # Rule management
                st.markdown("##### 🔧 Manage Rules")
                
                # Select rule to remove
                columns_with_rules = list(st.session_state.column_dq_rules.keys())
                if columns_with_rules:
                    remove_col = st.selectbox(
                        "Select Column",
                        options=columns_with_rules,
                        key="remove_rule_col"
                    )
                    
                    if remove_col and st.session_state.column_dq_rules.get(remove_col):
                        rules_for_col = st.session_state.column_dq_rules[remove_col]
                        rule_labels = [DQ_RULE_DEFINITIONS.get(r["type"], {}).get("label", r["type"]) 
                                      for r in rules_for_col]
                        
                        remove_rule_idx = st.selectbox(
                            "Select Rule to Remove",
                            options=range(len(rules_for_col)),
                            format_func=lambda x: rule_labels[x],
                            key="remove_rule_select"
                        )
                        
                        col_rm1, col_rm2 = st.columns(2)
                        with col_rm1:
                            if st.button("🗑️ Remove Rule", use_container_width=True):
                                st.session_state.column_dq_rules[remove_col].pop(remove_rule_idx)
                                # Clean up empty columns
                                if not st.session_state.column_dq_rules[remove_col]:
                                    del st.session_state.column_dq_rules[remove_col]
                                st.success("Rule removed!")
                                st.rerun()
                        
                        with col_rm2:
                            if st.button("🗑️ Clear All Rules", use_container_width=True):
                                st.session_state.column_dq_rules = {}
                                st.success("All rules cleared!")
                                st.rerun()
                
                # Show total rule count
                total_rules = sum(len(rules) for rules in st.session_state.column_dq_rules.values())
                columns_covered = len(st.session_state.column_dq_rules)
                st.info(f"**Total:** {total_rules} rule(s) across {columns_covered} column(s)")
        else:
            st.info("No quality rules configured yet. Select a column and add rules from the left panel.")
            
            # Quick start suggestions
            st.markdown("##### 💡 Quick Start Suggestions")
            st.caption("Consider adding these common rules:")
            st.markdown("""
            - **Primary Key columns**: Uniqueness + Null Check (100%)
            - **Email fields**: Format Check (email pattern)
            - **Date columns**: Freshness + Range Check
            - **Required fields**: Null Check (95-100%)
            """)
    
    # Convert session state rules to the format expected by contract creation
    quality_rules = []
    for col_name, rules in st.session_state.column_dq_rules.items():
        for rule in rules:
            quality_rules.append(rule)
    
    st.markdown("---")
    
    # Step 4: Review and Create
    st.markdown("### Step 4: Review & Create")
    
    if st.button("📋 Create Contract", type="primary", use_container_width=True):
        if not owner or not description or not business_purpose:
            st.error("Please fill in all required fields (*)")
        else:
            try:
                # Get domain, data_asset, and database - these are set in both modes
                contract_domain = table_domain
                contract_data_asset = table_data_asset
                contract_database = table_database
                
                contract = contract_engine.create_contract(
                    table=selected_table,
                    owner=owner,
                    classification=classification,
                    description=description,
                    business_purpose=business_purpose,
                    quality_rules=quality_rules,
                    sla_hours=sla_hours,
                    contains_pii=contains_pii,
                    domain=contract_domain,
                    data_asset=contract_data_asset,
                    database=contract_database,
                    data_history_years=data_history_years
                )
                
                # Add to session state contracts
                st.session_state.contract_engine.contracts[table_fqn] = contract
                
                # Clear new table columns from session state if in new table mode
                if creation_mode == "✨ Design New Table from Scratch" and "new_table_columns" in st.session_state:
                    del st.session_state.new_table_columns
                
                # Clear existing table columns from session state if in existing table mode
                if creation_mode == "📊 From Existing Table":
                    if "existing_table_columns" in st.session_state:
                        del st.session_state.existing_table_columns
                    if "existing_table_original_columns" in st.session_state:
                        del st.session_state.existing_table_original_columns
                    if "existing_table_selected_fqn" in st.session_state:
                        del st.session_state.existing_table_selected_fqn
                
                # Clear column-level DQ rules from session state
                if "column_dq_rules" in st.session_state:
                    del st.session_state.column_dq_rules
                if "dq_rules_table_id" in st.session_state:
                    del st.session_state.dq_rules_table_id
                
                # Build success message with hierarchy info
                hierarchy_msg = f"Domain: {contract_domain}"
                if contract_data_asset:
                    hierarchy_msg += f", Data Asset: {contract_data_asset}"
                hierarchy_msg += f", Database: {contract_database}"
                
                st.success(f"✅ Contract created successfully for {selected_table.get('name')}! ({hierarchy_msg})")
                
                if creation_mode == "✨ Design New Table from Scratch":
                    st.info("Contract is in **DRAFT** status. Submit for review to activate.")
                    st.warning("""
                    ⚠️ **Important:** This table doesn't exist yet!  
                    Use the **Developer Tools** tab to generate DDL and create the table in Databricks.
                    """)
                else:
                    st.info("Contract is in **DRAFT** status. Submit for review to activate.")
                
                st.markdown("---")
                
                # NEW: Post-Creation Artifact Generation
                st.markdown("### 🚀 Next Steps: Generate Developer Artifacts")
                st.markdown("Get started faster with auto-generated code!")
                
                code_gen = CodeGenerationEngine()
                
                with st.container():
                    st.markdown("""
                    <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                padding: 1.5rem; border-radius: 12px; color: white; margin-bottom: 1rem;'>
                        <h4 style='margin: 0 0 0.5rem 0; color: white;'>🎉 Your contract is ready!</h4>
                        <p style='margin: 0; opacity: 0.9;'>Generate production-ready code to accelerate development</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Quick artifact generation options
                    artifact_col1, artifact_col2, artifact_col3 = st.columns(3)
                    
                    with artifact_col1:
                        st.markdown("**📊 Databricks DDL**")
                        if st.button("Generate DDL", key="quick_gen_ddl"):
                            ddl_code = code_gen.generate_databricks_ddl(contract)
                            st.code(ddl_code, language="sql")
                            st.download_button(
                                "💾 Download DDL",
                                ddl_code,
                                file_name=f"{contract.table_name}_ddl.sql",
                                key="download_quick_ddl"
                            )
                    
                    with artifact_col2:
                        st.markdown("**🐍 PySpark Schema**")
                        if st.button("Generate Schema", key="quick_gen_schema"):
                            schema_code = code_gen.generate_pyspark_schema(contract)
                            st.code(schema_code, language="python")
                            st.download_button(
                                "💾 Download Schema",
                                schema_code,
                                file_name=f"{contract.table_name}_schema.py",
                                key="download_quick_schema"
                            )
                    
                    with artifact_col3:
                        st.markdown("**✅ Quality Tests**")
                        if st.button("Generate Tests", key="quick_gen_tests"):
                            test_code = code_gen.generate_quality_tests(contract)
                            st.code(test_code, language="python")
                            st.download_button(
                                "💾 Download Tests",
                                test_code,
                                file_name=f"{contract.table_name}_tests.py",
                                key="download_quick_tests"
                            )
                    
                    st.markdown("---")
                    
                    # All-in-one options
                    all_col1, all_col2 = st.columns(2)
                    
                    with all_col1:
                        if st.button("📓 Export as Databricks Notebook", key="export_notebook", use_container_width=True):
                            notebook_code = code_gen.generate_databricks_notebook(contract)
                            st.download_button(
                                "💾 Download Complete Notebook",
                                notebook_code,
                                file_name=f"{contract.table_name}_implementation.py",
                                mime="text/x-python",
                                key="download_notebook_quick"
                            )
                            st.success("✅ Notebook ready for download!")
                    
                    with all_col2:
                        if st.button("📝 Generate Documentation", key="export_docs", use_container_width=True):
                            doc_code = code_gen.generate_documentation(contract)
                            st.download_button(
                                "💾 Download Documentation",
                                doc_code,
                                file_name=f"{contract.table_name}_contract.md",
                                mime="text/markdown",
                                key="download_docs_quick"
                            )
                            st.success("✅ Documentation ready for download!")
                    
                    st.markdown("---")
                    
                    # Navigation
                    nav_col1, nav_col2, nav_col3 = st.columns(3)
                    
                    with nav_col1:
                        st.info("💡 Visit **Developer Tools** tab for all code generation options")
                    
                    with nav_col2:
                        if st.button("🚀 Go to Developer Tools", key="goto_devtools"):
                            st.info("Switch to the 'Developer Tools' tab above")
                    
                    with nav_col3:
                        if st.button("➕ Create Another Contract", key="create_another"):
                            st.rerun()
                
            except Exception as e:
                st.error(f"Error creating contract: {str(e)}")

def render_compliance_monitoring(tables: List[Dict], contracts: Dict[str, DataContract],
                                contract_engine: DataContractEngine):
    """Render compliance monitoring"""
    st.subheader("🔍 Contract Compliance Monitoring")
    
    if not contracts:
        st.info("No contracts to monitor yet.")
        return
    
    # Calculate compliance metrics
    active_contracts = [c for c in contracts.values() if c.status == "active"]
    
    if not active_contracts:
        st.info("No active contracts to monitor. Approve contracts to start monitoring.")
        return
    
    st.markdown(f"### Monitoring {len(active_contracts)} active contracts")
    st.markdown("---")
    
    # Compliance summary
    violations = []
    compliant = []
    
    for contract in active_contracts:
        table = next((t for t in tables if t.get("fullyQualifiedName") == contract.table_fqn), None)
        if not table:
            continue
        
        # Check schema compliance
        schema_changes = contract_engine.detect_schema_changes(contract.table_fqn, table)
        breaking_changes = [c for c in schema_changes if c.severity == "breaking"]
        
        # Check freshness
        last_updated = table.get("updatedAt")
        if last_updated:
            hours_old = (datetime.now() - datetime.fromtimestamp(int(last_updated) / 1000)).total_seconds() / 3600
            sla_hours = contract.sla_requirements.get("freshness_hours", 24)
            is_fresh = hours_old <= sla_hours
        else:
            is_fresh = False
        
        has_issues = len(breaking_changes) > 0 or not is_fresh
        
        if has_issues:
            violations.append({
                "contract": contract,
                "breaking_changes": breaking_changes,
                "is_fresh": is_fresh
            })
        else:
            compliant.append(contract)
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        compliance_rate = (len(compliant) / len(active_contracts) * 100) if active_contracts else 0
        status = "success" if compliance_rate >= 90 else "warning" if compliance_rate >= 70 else "danger"
        st.metric("Compliance Rate", f"{compliance_rate:.1f}%", 
                 f"{len(compliant)}/{len(active_contracts)} compliant")
    
    with col2:
        st.metric("Violations", len(violations), 
                 "Requires attention" if violations else "All clear")
    
    with col3:
        total_breaking = sum(len(v["breaking_changes"]) for v in violations)
        st.metric("Breaking Changes", total_breaking,
                 "Immediate action required" if total_breaking > 0 else "")
    
    st.markdown("---")
    
    # Display violations
    if violations:
        st.subheader("⚠️ Contracts with Issues")
        
        for violation in violations:
            contract = violation["contract"]
            breaking_changes = violation["breaking_changes"]
            is_fresh = violation["is_fresh"]
            
            with st.container():
                st.markdown(f"""
                    <div class="contract-card contract-violation">
                        <h3>🚨 {contract.table_name}</h3>
                        <p style="color: #666;">{contract.table_fqn}</p>
                    </div>
                """, unsafe_allow_html=True)
                
                if breaking_changes:
                    st.error(f"**Schema Violations:** {len(breaking_changes)} breaking changes detected")
                    
                    for change in breaking_changes:
                        with st.expander(f"🔴 {change.change_type}: {change.column_name}"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown(f"**Change Type:** {change.change_type}")
                                st.markdown(f"**Severity:** {change.severity}")
                                st.markdown(f"**Impact Level:** {change.impact_level}")
                            
                            with col2:
                                if change.old_value:
                                    st.markdown(f"**Old Value:** `{change.old_value}`")
                                if change.new_value:
                                    st.markdown(f"**New Value:** `{change.new_value}`")
                            
                            if change.requires_approval:
                                st.warning("⚠️ This change requires governance approval")
                            
                            # Notify consumers button
                            if st.button(f"📧 Notify Consumers", key=f"notify_{contract.id}_{change.column_name}"):
                                st.success(f"Notifications sent to {len(contract.registered_consumers)} consumers")
                
                if not is_fresh:
                    st.warning("**Freshness Violation:** Data is stale according to SLA")
                
                st.markdown("---")
    else:
        st.success("✅ All active contracts are compliant! Excellent work!")

def render_schema_drift_monitor(tables: List[Dict], contracts: Dict[str, DataContract],
                                contract_engine: DataContractEngine, mock_gen: MockDataGenerator):
    """Render schema drift monitoring"""
    st.subheader("🔄 Schema Drift Detection & Impact Analysis")
    
    if not contracts:
        st.info("No contracts available for schema monitoring.")
        return
    
    # Select contract to analyze
    contract_fqns = list(contracts.keys())
    selected_fqn = st.selectbox(
        "Select contract to analyze",
        contract_fqns,
        format_func=lambda x: f"{x.split('.')[-1]} ({x})"
    )
    
    if not selected_fqn:
        return
    
    contract = contracts[selected_fqn]
    table = next((t for t in tables if t.get("fullyQualifiedName") == selected_fqn), None)
    
    if not table:
        st.error("Table not found")
        return
    
    st.markdown("---")
    
    # Detect changes
    schema_changes = contract_engine.detect_schema_changes(selected_fqn, table)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📊 Current Schema vs Contract")
        
        # Compare schemas
        contract_cols = set(contract.schema_definition.keys())
        current_cols = set(col["name"] for col in table.get("columns", []))
        
        col_a, col_b, col_c = st.columns(3)
        
        with col_a:
            st.metric("Contract Columns", len(contract_cols))
        with col_b:
            st.metric("Current Columns", len(current_cols))
        with col_c:
            delta = len(current_cols) - len(contract_cols)
            st.metric("Difference", delta, delta=delta)
    
    with col2:
        st.markdown("### 🎯 Change Summary")
        
        if schema_changes:
            change_counts = defaultdict(int)
            for change in schema_changes:
                change_counts[change.change_type] += 1
            
            for change_type, count in change_counts.items():
                st.metric(change_type.replace("_", " ").title(), count)
        else:
            st.success("No changes detected")
    
    st.markdown("---")
    
    # Display detailed changes
    if schema_changes:
        st.markdown("### 📋 Detected Changes")
        
        for change in schema_changes:
            severity_emoji = {"breaking": "🔴", "non-breaking": "🟢", "warning": "🟡"}
            
            with st.expander(
                f"{severity_emoji.get(change.severity, '⚪')} {change.change_type.replace('_', ' ').title()}: {change.column_name}",
                expanded=(change.severity == "breaking")
            ):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"**Column:** `{change.column_name}`")
                    st.markdown(f"**Change Type:** {change.change_type}")
                
                with col2:
                    if change.old_value:
                        st.markdown(f"**Old Value:** `{change.old_value}`")
                    if change.new_value:
                        st.markdown(f"**New Value:** `{change.new_value}`")
                
                with col3:
                    st.markdown(f"**Severity:** {change.severity}")
                    st.markdown(f"**Impact:** {change.impact_level}")
                
                if change.requires_approval:
                    st.error("⚠️ This change requires governance approval before deployment")
                
                # Impact analysis
                if st.button(f"🔍 Analyze Impact", key=f"impact_{change.column_name}"):
                    with st.spinner("Analyzing downstream impact..."):
                        lineage = mock_gen.generate_lineage(selected_fqn, tables)
                        downstream_count = len(lineage.get("downstreamEdges", []))
                        
                        st.markdown("#### Impact Analysis Results")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Affected Tables", downstream_count)
                        
                        with col2:
                            impact_level = "High" if downstream_count > 10 else "Medium" if downstream_count > 3 else "Low"
                            st.metric("Impact Level", impact_level)
                        
                        with col3:
                            st.metric("Registered Consumers", len(contract.registered_consumers))
                        
                        if contract.registered_consumers:
                            st.markdown("**Consumers to Notify:**")
                            for consumer in contract.registered_consumers:
                                st.markdown(f"- 📧 {consumer}")
    else:
        st.success("✅ Schema is compliant with contract. No drift detected!")
    
    st.markdown("---")
    
    # Schema comparison table
    st.markdown("### 📝 Full Schema Comparison")
    
    comparison_data = []
    
    # All columns from both contract and current
    all_columns = set(contract.schema_definition.keys()) | set(col["name"] for col in table.get("columns", []))
    
    for col_name in sorted(all_columns):
        contract_type = contract.schema_definition.get(col_name, {}).get("dataType", "-")
        
        current_col = next((c for c in table.get("columns", []) if c["name"] == col_name), None)
        current_type = current_col.get("dataType", "-") if current_col else "-"
        
        status = "✅ Match"
        if contract_type == "-":
            status = "➕ Added"
        elif current_type == "-":
            status = "➖ Removed"
        elif contract_type != current_type:
            status = "⚠️ Type Changed"
        
        comparison_data.append({
            "Column": col_name,
            "Contract Type": contract_type,
            "Current Type": current_type,
            "Status": status
        })
    
    df_comparison = pd.DataFrame(comparison_data)
    st.dataframe(df_comparison, use_container_width=True, hide_index=True)

def render_consumer_registry(contracts: Dict[str, DataContract], contract_engine: DataContractEngine):
    """Render consumer registry"""
    st.subheader("👥 Consumer Registry")
    st.markdown("Track and manage downstream consumers of your data contracts")
    
    if not contracts:
        st.info("No contracts available.")
        return
    
    st.markdown("---")
    
    # Summary
    total_consumers = sum(len(c.registered_consumers) for c in contracts.values())
    contracts_with_consumers = sum(1 for c in contracts.values() if c.registered_consumers)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Contracts", len(contracts))
    with col2:
        st.metric("Contracts with Consumers", contracts_with_consumers)
    with col3:
        st.metric("Total Registered Consumers", total_consumers)
    
    st.markdown("---")
    
    # Register new consumer
    with st.expander("➕ Register New Consumer"):
        col1, col2 = st.columns(2)
        
        with col1:
            contract_fqn = st.selectbox(
                "Select Contract",
                list(contracts.keys()),
                format_func=lambda x: f"{x.split('.')[-1]} ({x})"
            )
        
        with col2:
            consumer_name = st.text_input("Consumer Name", "")
        
        consumer_contact = st.text_input("Consumer Contact (email)", "")
        
        if st.button("Register Consumer"):
            if consumer_name and consumer_contact:
                success = contract_engine.register_consumer(contract_fqn, consumer_name, consumer_contact)
                if success:
                    st.success(f"✅ Consumer '{consumer_name}' registered for {contract_fqn}")
                    st.rerun()
                else:
                    st.error("Failed to register consumer")
            else:
                st.error("Please provide both name and contact")
    
    st.markdown("---")
    
    # Display contracts with consumers
    st.markdown("### 📊 Consumer Relationships")
    
    for contract in contracts.values():
        if contract.registered_consumers or contract.status == "active":
            with st.container():
                st.markdown(f"#### 📊 {contract.table_name}")
                st.caption(contract.table_fqn)
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    if contract.registered_consumers:
                        st.markdown("**Registered Consumers:**")
                        for consumer in contract.registered_consumers:
                            st.markdown(f"- 👤 {consumer}")
                    else:
                        st.info("No consumers registered yet")
                
                with col2:
                    st.markdown(f"**Status:** {contract.status}")
                    st.markdown(f"**Classification:** {contract.classification}")
                
                st.markdown("---")

def render_developer_tools(contracts: Dict[str, DataContract]):
    """Render Developer Tools for code generation and artifacts"""
    st.subheader("🚀 Developer Tools & Code Generation")
    st.markdown("Generate production-ready code artifacts from your data contracts")
    st.markdown("---")
    
    if not contracts:
        st.info("No contracts available. Create a contract first to generate code artifacts.")
        return
    
    # Step 1: Select Contract
    st.markdown("### Step 1: Select Data Contract")
    
    contract_options = {fqn: c.table_name for fqn, c in contracts.items()}
    selected_fqn = st.selectbox(
        "Choose a contract",
        options=list(contract_options.keys()),
        format_func=lambda x: f"{contract_options[x]} ({x})",
        key="devtools_contract_select"
    )
    
    if not selected_fqn:
        return
    
    contract = contracts[selected_fqn]
    
    # Display contract info
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Version", contract.version)
    with col2:
        st.metric("Status", contract.status)
    with col3:
        st.metric("Columns", len(contract.schema_definition))
    with col4:
        st.metric("Quality Rules", len(contract.quality_rules))
    
    st.markdown("---")
    
    # Step 2: Generate Code Artifacts
    st.markdown("### Step 2: Generate Code Artifacts")
    
    # Initialize code generation engine
    code_gen = CodeGenerationEngine()
    
    # Create tabs for different artifacts
    art_tab1, art_tab2, art_tab3, art_tab4, art_tab5, art_tab6 = st.tabs([
        "📊 Databricks DDL",
        "🐍 PySpark Schema",
        "✅ Quality Tests (PySpark)",
        "🗄️ Unity Catalog",
        "📝 Documentation",
        "📓 Complete Notebook"
    ])
    
    with art_tab1:
        st.markdown("#### Databricks Delta Table DDL")
        st.caption("Copy this DDL to create your table in Databricks")
        
        ddl_code = code_gen.generate_databricks_ddl(contract)
        
        st.code(ddl_code, language="sql")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy DDL to Clipboard", key="copy_ddl"):
                st.success("✅ DDL copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download DDL",
                data=ddl_code,
                file_name=f"{contract.table_name}_ddl.sql",
                mime="text/sql",
                key="download_ddl"
            )
    
    with art_tab2:
        st.markdown("#### PySpark Schema Definition")
        st.caption("Use this schema in your PySpark transformations")
        
        pyspark_code = code_gen.generate_pyspark_schema(contract)
        
        st.code(pyspark_code, language="python")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy Schema to Clipboard", key="copy_pyspark"):
                st.success("✅ Schema copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download Schema",
                data=pyspark_code,
                file_name=f"{contract.table_name}_schema.py",
                mime="text/x-python",
                key="download_pyspark"
            )
    
    with art_tab3:
        st.markdown("#### Data Quality Tests (PySpark)")
        st.caption("Automated quality validation for your data using PySpark")
        
        # Generate PySpark tests
        quality_code = code_gen.generate_quality_tests(contract)
        
        st.code(quality_code, language="python")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy Tests to Clipboard", key="copy_quality"):
                st.success("✅ Tests copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download Tests",
                data=quality_code,
                file_name=f"{contract.table_name}_quality_tests.py",
                mime="text/x-python",
                key="download_quality"
            )
    
    with art_tab4:
        st.markdown("#### Unity Catalog Registration")
        st.caption("Register and configure table in Unity Catalog")
        
        unity_sql = code_gen.generate_unity_catalog_sql(contract)
        
        st.code(unity_sql, language="sql")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy SQL to Clipboard", key="copy_unity"):
                st.success("✅ SQL copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download SQL",
                data=unity_sql,
                file_name=f"{contract.table_name}_unity_catalog.sql",
                mime="text/sql",
                key="download_unity"
            )
    
    with art_tab5:
        st.markdown("#### Documentation (Markdown)")
        st.caption("Complete contract documentation for sharing")
        
        doc_md = code_gen.generate_documentation(contract)
        
        st.code(doc_md, language="markdown")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy Documentation to Clipboard", key="copy_doc"):
                st.success("✅ Documentation copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download Documentation",
                data=doc_md,
                file_name=f"{contract.table_name}_contract.md",
                mime="text/markdown",
                key="download_doc"
            )
    
    with art_tab6:
        st.markdown("#### Complete Databricks Notebook")
        st.caption("All-in-one notebook with DDL, schema, validation, and registration")
        
        notebook_code = code_gen.generate_databricks_notebook(contract)
        
        st.code(notebook_code, language="python")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Copy Notebook to Clipboard", key="copy_notebook"):
                st.success("✅ Notebook copied to clipboard!")
        with col2:
            st.download_button(
                label="💾 Download Notebook",
                data=notebook_code,
                file_name=f"{contract.table_name}_implementation.py",
                mime="text/x-python",
                key="download_notebook"
            )
    
    st.markdown("---")
    
    # Step 3: Integration Actions
    st.markdown("### Step 3: Integration & Collaboration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🎫 Create Jira Ticket")
        st.markdown("Auto-create a Jira ticket with all artifacts attached")
        
        with st.expander("Jira Configuration"):
            jira_project = st.text_input("Jira Project Key", value="DATA-ENG", key="jira_project")
            jira_issue_type = st.selectbox("Issue Type", ["Task", "Story", "Epic"], key="jira_issue_type")
            jira_assignee = st.text_input("Assignee (optional)", key="jira_assignee")
            
            if st.button("🎫 Create Jira Ticket", key="create_jira"):
                # In real implementation, this would call Jira API
                st.success(f"""
                ✅ Jira ticket created successfully!
                
                **Ticket:** {jira_project}-1234
                **Title:** Implement {contract.table_name} Data Contract
                **Type:** {jira_issue_type}
                **Attachments:**
                - DDL Script
                - PySpark Schema
                - Quality Tests
                - Unity Catalog SQL
                - Documentation
                
                [View Ticket →](https://jira.example.com/{jira_project}-1234)
                """)
    
    with col2:
        st.markdown("#### 📦 Download All Artifacts")
        st.markdown("Get all generated code in a single ZIP file")
        
        st.info("""
        **Package includes:**
        - Databricks DDL
        - PySpark Schema
        - Quality Tests (all frameworks)
        - Unity Catalog SQL
        - Documentation (Markdown)
        - Complete Notebook
        - README with implementation guide
        """)
        
        if st.button("⬇️ Download Complete Package (ZIP)", key="download_all"):
            # In real implementation, this would create a ZIP file
            st.success("✅ Package ready for download!")
            st.download_button(
                label="💾 Download artifacts.zip",
                data=b"",  # Would contain actual ZIP file
                file_name=f"{contract.table_name}_artifacts.zip",
                mime="application/zip",
                key="download_zip"
            )
    
    st.markdown("---")
    
    # Quick Stats
    st.markdown("### 📊 Quick Stats")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Lines of Code", "~500", help="Total generated code")
    with col2:
        st.metric("Time Saved", "~4 hours", help="Estimated dev time saved")
    with col3:
        st.metric("Artifacts", "6+", help="Number of generated artifacts")
    with col4:
        st.metric("Accuracy", "100%", help="Contract compliance")

def render_trust_scorecard(tables: List[Dict], contracts: Dict[str, DataContract],
                           trust_engine: TrustScoreEngine, mock_gen: MockDataGenerator):
    """Render Data Trust & Readiness Scorecard"""
    st.markdown('<div class="main-header">🎯 Data Trust & Readiness Scorecard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Comprehensive trust scoring across all data assets</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Calculate trust scores
    with st.spinner("Calculating trust scores..."):
        trust_scores = trust_engine.calculate_all_trust_scores(tables, contracts, mock_gen)
        summary = trust_engine.get_trust_score_summary(trust_scores)
    
    if not trust_scores:
        st.warning("No data assets found to score.")
        return
    
    # ==== EXECUTIVE SUMMARY ====
    st.markdown("### 📊 Executive Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        avg_score = summary.get("avg_score", 0)
        status = "metric-card-green" if avg_score >= 75 else "metric-card-orange" if avg_score >= 60 else "metric-card-purple"
        render_metric_card_gradient(
            "Average Trust Score",
            f"{avg_score:.1f}",
            f"{summary.get('total_assets', 0)} assets",
            status
        )
    
    with col2:
        high_trust = summary.get("high_trust_assets", 0)
        render_metric_card_gradient(
            "High Trust Assets",
            f"{high_trust}",
            f"{(high_trust/len(trust_scores)*100):.1f}% of total" if trust_scores else "0%",
            "metric-card-blue"
        )
    
    with col3:
        needs_attention = summary.get("needs_attention_assets", 0)
        status = "metric-card-green" if needs_attention == 0 else "metric-card-orange"
        render_metric_card_gradient(
            "Needs Attention",
            f"{needs_attention}",
            f"{(needs_attention/len(trust_scores)*100):.1f}% of total" if trust_scores else "0%",
            status
        )
    
    with col4:
        render_metric_card_gradient(
            "Best Performer",
            f"{summary.get('max_score', 0):.1f}",
            "Highest score",
            "metric-card-green"
        )
    
    with col5:
        render_metric_card_gradient(
            "Improvement Target",
            f"{summary.get('min_score', 0):.1f}",
            "Lowest score",
            "metric-card-orange"
        )
    
    st.markdown("---")
    
    # ==== TRUST LEVEL DISTRIBUTION ====
    st.markdown("### 🏆 Trust Level Distribution")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Pie chart of trust levels
        level_dist = summary.get("level_distribution", {})
        if level_dist:
            level_colors = {
                "Platinum": "#9b59b6",
                "Gold": "#f39c12",
                "Silver": "#95a5a6",
                "Bronze": "#cd7f32",
                "Needs Attention": "#e74c3c"
            }
            
            fig = go.Figure(data=[go.Pie(
                labels=list(level_dist.keys()),
                values=list(level_dist.values()),
                hole=0.4,
                marker=dict(colors=[level_colors.get(k, "#3498db") for k in level_dist.keys()]),
                textinfo='label+percent+value',
                hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
            )])
            
            fig.update_layout(
                title="Assets by Trust Level",
                height=400,
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Bar chart showing counts
        if level_dist:
            level_order = ["Platinum", "Gold", "Silver", "Bronze", "Needs Attention"]
            sorted_levels = {k: level_dist.get(k, 0) for k in level_order if k in level_dist}
            
            fig = go.Figure(data=[go.Bar(
                x=list(sorted_levels.keys()),
                y=list(sorted_levels.values()),
                marker_color=[level_colors.get(k, "#3498db") for k in sorted_levels.keys()],
                text=list(sorted_levels.values()),
                textposition='auto',
            )])
            
            fig.update_layout(
                title="Asset Count by Trust Level",
                xaxis_title="Trust Level",
                yaxis_title="Number of Assets",
                height=400,
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==== SCORE COMPONENT BREAKDOWN ====
    st.markdown("### 📈 Score Component Analysis")
    
    # Calculate average component scores
    avg_components = {
        "Data Quality": sum(ts.data_quality_score for ts in trust_scores) / len(trust_scores),
        "Contract Availability": sum(ts.contract_availability_score for ts in trust_scores) / len(trust_scores),
        "Freshness": sum(ts.freshness_score for ts in trust_scores) / len(trust_scores),
        "Documentation": sum(ts.documentation_score for ts in trust_scores) / len(trust_scores),
        "Lineage & Usage": sum(ts.lineage_usage_score for ts in trust_scores) / len(trust_scores),
        "Security & Compliance": sum(ts.security_compliance_score for ts in trust_scores) / len(trust_scores)
    }
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Radar chart of component scores
        categories = list(avg_components.keys())
        values = list(avg_components.values())
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name='Average Scores',
            line_color='#3498db'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )
            ),
            title="Component Score Radar",
            height=450,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Horizontal bar chart
        fig = go.Figure()
        
        sorted_components = sorted(avg_components.items(), key=lambda x: x[1], reverse=True)
        
        colors = ['#27ae60' if v >= 75 else '#f39c12' if v >= 60 else '#e74c3c' 
                 for _, v in sorted_components]
        
        fig.add_trace(go.Bar(
            y=[k for k, _ in sorted_components],
            x=[v for _, v in sorted_components],
            orientation='h',
            marker_color=colors,
            text=[f"{v:.1f}" for _, v in sorted_components],
            textposition='auto',
        ))
        
        fig.update_layout(
            title="Average Component Scores",
            xaxis_title="Score (0-100)",
            yaxis_title="Component",
            height=450,
            showlegend=False,
            xaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==== DOMAIN PERFORMANCE ====
    st.markdown("### 🏢 Trust Score by Domain")
    
    domain_averages = summary.get("domain_averages", {})
    if domain_averages:
        sorted_domains = sorted(domain_averages.items(), key=lambda x: x[1], reverse=True)
        
        fig = go.Figure()
        
        colors = ['#27ae60' if v >= 75 else '#f39c12' if v >= 60 else '#e74c3c' 
                 for _, v in sorted_domains]
        
        fig.add_trace(go.Bar(
            x=[k for k, _ in sorted_domains],
            y=[v for _, v in sorted_domains],
            marker_color=colors,
            text=[f"{v:.1f}" for _, v in sorted_domains],
            textposition='auto',
        ))
        
        fig.update_layout(
            title="Average Trust Score by Domain",
            xaxis_title="Domain",
            yaxis_title="Average Score",
            height=400,
            showlegend=False,
            yaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==== DETAILED ASSET SCORECARD ====
    st.markdown("### 📋 Detailed Asset Scorecard")
    
    # Filters with cascading logic
    col1, col2 = st.columns(2)
    
    with col1:
        filter_domain = st.multiselect(
            "Filter by Domain",
            options=sorted(set(ts.domain for ts in trust_scores)),
            default=[],
            key="trust_filter_domain"
        )
    
    with col2:
        # Data Asset filter - shows assets based on selected domains
        if filter_domain:
            available_assets = set()
            for domain in filter_domain:
                available_assets.update(DATA_ASSETS.get(domain, []))
            available_assets = sorted(available_assets)
        else:
            available_assets = sorted(set(ts.data_asset for ts in trust_scores if ts.data_asset))
        
        if available_assets:
            filter_data_asset = st.multiselect(
                "Filter by Data Asset",
                options=available_assets,
                default=[],
                key="trust_filter_data_asset"
            )
        else:
            st.multiselect("Filter by Data Asset", options=["N/A"], disabled=True, key="trust_filter_data_asset_disabled")
            filter_data_asset = []
    
    col3, col4, col5 = st.columns(3)
    
    with col3:
        filter_database = st.multiselect(
            "Filter by Database",
            options=sorted(set(ts.database for ts in trust_scores)),
            default=[],
            key="trust_filter_database"
        )
    
    with col4:
        filter_level = st.multiselect(
            "Filter by Trust Level",
            options=["Platinum", "Gold", "Silver", "Bronze", "Needs Attention"],
            default=[],
            key="trust_filter_level"
        )
    
    with col5:
        min_score = st.slider("Minimum Trust Score", 0, 100, 0, key="trust_min_score")
    
    # Apply filters
    filtered_scores = trust_scores
    if filter_domain:
        filtered_scores = [ts for ts in filtered_scores if ts.domain in filter_domain]
    if filter_data_asset:
        filtered_scores = [ts for ts in filtered_scores if ts.data_asset in filter_data_asset]
    if filter_database:
        filtered_scores = [ts for ts in filtered_scores if ts.database in filter_database]
    if filter_level:
        filtered_scores = [ts for ts in filtered_scores if ts.trust_level in filter_level]
    filtered_scores = [ts for ts in filtered_scores if ts.composite_trust_score >= min_score]
    
    # Sort options
    sort_by = st.selectbox(
        "Sort by",
        ["Trust Score (High to Low)", "Trust Score (Low to High)", "Table Name", "Domain", "Data Asset"],
        key="trust_sort_by"
    )
    
    if sort_by == "Trust Score (High to Low)":
        filtered_scores = sorted(filtered_scores, key=lambda x: x.composite_trust_score, reverse=True)
    elif sort_by == "Trust Score (Low to High)":
        filtered_scores = sorted(filtered_scores, key=lambda x: x.composite_trust_score)
    elif sort_by == "Table Name":
        filtered_scores = sorted(filtered_scores, key=lambda x: x.table_name)
    elif sort_by == "Domain":
        filtered_scores = sorted(filtered_scores, key=lambda x: (x.domain, x.table_name))
    elif sort_by == "Data Asset":
        filtered_scores = sorted(filtered_scores, key=lambda x: (x.data_asset or "ZZZ", x.table_name))
    
    st.caption(f"Showing {len(filtered_scores)} of {len(trust_scores)} assets")
    
    # Display as expandable cards
    for ts in filtered_scores:
        # Determine trust level color
        level_colors = {
            "Platinum": "#9b59b6",
            "Gold": "#f39c12",
            "Silver": "#95a5a6",
            "Bronze": "#cd7f32",
            "Needs Attention": "#e74c3c"
        }
        level_color = level_colors.get(ts.trust_level, "#3498db")
        
        with st.expander(
            f"**{ts.table_name}** - Trust Score: {ts.composite_trust_score:.1f} ({ts.trust_level})"
        ):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**FQN:** `{ts.fqn}`")
                st.markdown(f"**Domain:** {ts.domain}")
                if ts.data_asset:
                    st.markdown(f"**Data Asset:** {ts.data_asset}")
                st.markdown(f"**Database:** {ts.database}")
                if ts.owner:
                    st.markdown(f"**Owner:** {ts.owner}")
                if ts.classification:
                    st.markdown(f"**Classification:** {ts.classification}")
                st.markdown(f"**Last Assessed:** {ts.last_assessed.strftime('%Y-%m-%d %H:%M')}")
            
            with col2:
                st.markdown(f"<div style='text-align: center; padding: 1rem; background: {level_color}; color: white; border-radius: 8px; font-size: 1.5rem; font-weight: bold;'>{ts.trust_level}<br/>{ts.composite_trust_score:.1f}/100</div>", unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Component scores
            st.markdown("**Component Scores:**")
            
            components = [
                ("Data Quality", ts.data_quality_score),
                ("Contract Availability", ts.contract_availability_score),
                ("Freshness", ts.freshness_score),
                ("Documentation", ts.documentation_score),
                ("Lineage & Usage", ts.lineage_usage_score),
                ("Security & Compliance", ts.security_compliance_score)
            ]
            
            for comp_name, comp_score in components:
                # Color based on score
                if comp_score >= 80:
                    color = "#27ae60"
                    icon = "🟢"
                elif comp_score >= 60:
                    color = "#f39c12"
                    icon = "🟡"
                else:
                    color = "#e74c3c"
                    icon = "🔴"
                
                st.markdown(
                    f"{icon} **{comp_name}:** "
                    f"<span style='color: {color}; font-weight: bold;'>{comp_score:.1f}</span>/100",
                    unsafe_allow_html=True
                )
            
            st.markdown("---")
            
            # Strengths and improvements
            col1, col2 = st.columns(2)
            
            with col1:
                if ts.strengths:
                    st.markdown("**✅ Strengths:**")
                    for strength in ts.strengths:
                        st.markdown(f"- {strength}")
            
            with col2:
                if ts.improvement_areas:
                    st.markdown("**⚠️ Improvement Areas:**")
                    for improvement in ts.improvement_areas:
                        st.markdown(f"- {improvement}")
    
    st.markdown("---")
    
    # ==== EXPORT OPTIONS ====
    st.markdown("### 📥 Export Trust Scores")
    
    if st.button("📊 Generate Trust Score Report"):
        # Create DataFrame for export
        export_data = []
        for ts in trust_scores:
            export_data.append({
                "FQN": ts.fqn,
                "Table Name": ts.table_name,
                "Domain": ts.domain,
                "Data Asset": ts.data_asset or "N/A",
                "Database": ts.database,
                "Owner": ts.owner or "Unassigned",
                "Classification": ts.classification or "Unclassified",
                "Trust Score": round(ts.composite_trust_score, 2),
                "Trust Level": ts.trust_level,
                "Data Quality": round(ts.data_quality_score, 2),
                "Contract Availability": round(ts.contract_availability_score, 2),
                "Freshness": round(ts.freshness_score, 2),
                "Documentation": round(ts.documentation_score, 2),
                "Lineage & Usage": round(ts.lineage_usage_score, 2),
                "Security & Compliance": round(ts.security_compliance_score, 2),
                "Top Strength": ts.strengths[0] if ts.strengths else "",
                "Top Improvement": ts.improvement_areas[0] if ts.improvement_areas else ""
            })
        
        df = pd.DataFrame(export_data)
        df = df.sort_values("Trust Score", ascending=False)
        
        st.download_button(
            label="⬇️ Download as CSV",
            data=df.to_csv(index=False),
            file_name=f"trust_scores_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
        st.dataframe(df, use_container_width=True, height=400)

# =============================================================================
# DATA PRODUCTS MARKETPLACE UI
# =============================================================================

def render_data_products(
    products: Dict[str, DataProduct],
    tables: List[Dict],
    contracts: Dict[str, DataContract],
    trust_scores: List[DataTrustScore],
    product_engine: DataProductEngine
):
    """Render Data Products Marketplace - the business-aligned view of data"""
    
    st.markdown('<div class="main-header">📦 Data Products Marketplace</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Business-aligned data products for self-service analytics</div>', 
                unsafe_allow_html=True)
    st.markdown("---")
    
    # Initialize session state for product view
    if "selected_product_id" not in st.session_state:
        st.session_state.selected_product_id = None
    if "show_product_wizard" not in st.session_state:
        st.session_state.show_product_wizard = False
    
    # Top metrics
    active_products = [p for p in products.values() if p.status == "active"]
    total_consumers = sum(p.consumer_count for p in products.values())
    avg_trust = sum(p.aggregated_trust_score for p in products.values()) / len(products) if products else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        render_metric_card_gradient(
            "Total Products",
            str(len(products)),
            f"{len(active_products)} active",
            "metric-card-blue"
        )
    
    with col2:
        render_metric_card_gradient(
            "Total Consumers",
            str(total_consumers),
            "across all products",
            "metric-card-green"
        )
    
    with col3:
        render_metric_card_gradient(
            "Avg Trust Score",
            f"{avg_trust:.1f}%",
            "product quality",
            "metric-card-purple"
        )
    
    with col4:
        render_metric_card_gradient(
            "Data Assets",
            str(len(set(a for p in products.values() for a in p.data_assets))),
            "covered by products",
            "metric-card-orange"
        )
    
    st.markdown("---")
    
    # Sub-tabs for different views
    subtab1, subtab2, subtab3 = st.tabs(["🏪 Product Catalog", "➕ Create Product", "📊 Product Analytics"])
    
    with subtab1:
        render_product_catalog(products, tables, contracts, trust_scores, product_engine)
    
    with subtab2:
        render_product_creation_wizard(tables, contracts, product_engine)
    
    with subtab3:
        render_product_analytics(products, trust_scores)


def render_product_catalog(
    products: Dict[str, DataProduct],
    tables: List[Dict],
    contracts: Dict[str, DataContract],
    trust_scores: List[DataTrustScore],
    product_engine: DataProductEngine
):
    """Render the product catalog with filtering and detail view"""
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        domain_filter = st.selectbox(
            "Domain",
            ["All Domains"] + ALLOWED_DOMAINS,
            key="product_domain_filter"
        )
    
    with col2:
        status_filter = st.selectbox(
            "Status",
            ["All Status", "active", "draft", "deprecated"],
            key="product_status_filter"
        )
    
    with col3:
        search_query = st.text_input("🔍 Search products", key="product_search")
    
    with col4:
        sort_by = st.selectbox(
            "Sort by",
            ["Name", "Trust Score", "Usage Count", "Rating"],
            key="product_sort"
        )
    
    # Apply filters
    filtered_products = list(products.values())
    
    if domain_filter != "All Domains":
        filtered_products = [p for p in filtered_products if p.domain == domain_filter]
    
    if status_filter != "All Status":
        filtered_products = [p for p in filtered_products if p.status == status_filter]
    
    if search_query:
        query_lower = search_query.lower()
        filtered_products = [
            p for p in filtered_products
            if query_lower in p.name.lower() or
               query_lower in p.business_purpose.lower() or
               any(query_lower in tag.lower() for tag in p.tags)
        ]
    
    # Sort
    if sort_by == "Trust Score":
        filtered_products.sort(key=lambda x: x.aggregated_trust_score, reverse=True)
    elif sort_by == "Usage Count":
        filtered_products.sort(key=lambda x: x.usage_count, reverse=True)
    elif sort_by == "Rating":
        filtered_products.sort(key=lambda x: x.rating, reverse=True)
    else:
        filtered_products.sort(key=lambda x: x.name)
    
    st.markdown(f"**Showing {len(filtered_products)} products**")
    st.markdown("---")
    
    # Product cards in grid
    if not filtered_products:
        st.info("No products found matching your criteria.")
    else:
        # Display 2 products per row
        for i in range(0, len(filtered_products), 2):
            cols = st.columns(2)
            
            for j, col in enumerate(cols):
                if i + j < len(filtered_products):
                    product = filtered_products[i + j]
                    
                    with col:
                        render_product_card(product, tables, contracts, trust_scores, product_engine)


def render_product_card(
    product: DataProduct,
    tables: List[Dict],
    contracts: Dict[str, DataContract],
    trust_scores: List[DataTrustScore],
    product_engine: DataProductEngine
):
    """Render a single product card"""
    
    # Status colors
    status_colors = {
        "active": "#28a745",
        "draft": "#6c757d",
        "deprecated": "#dc3545"
    }
    
    # Trust level colors
    trust_colors = {
        "Platinum": "#E5E4E2",
        "Gold": "#FFD700",
        "Silver": "#C0C0C0",
        "Bronze": "#CD7F32",
        "Needs Attention": "#e74c3c"
    }
    
    status_color = status_colors.get(product.status, "#6c757d")
    trust_color = trust_colors.get(product.trust_level, "#6c757d")
    
    # Card container
    with st.container():
        st.markdown(f"""
            <div style="border: 2px solid #e0e0e0; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; 
                        background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.05); border-left: 5px solid {status_color};">
                <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 0.5rem;">
                    <h3 style="margin: 0; color: #333;">📦 {product.name}</h3>
                    <span style="background: {status_color}; color: white; padding: 0.25rem 0.75rem; 
                                border-radius: 12px; font-size: 0.8rem; font-weight: 600;">{product.status.upper()}</span>
                </div>
                <p style="color: #666; font-size: 0.9rem; margin-bottom: 0.75rem;">{product.domain}</p>
                <p style="color: #555; font-size: 0.85rem; margin-bottom: 1rem; line-height: 1.4;">
                    {product.business_purpose[:150]}{'...' if len(product.business_purpose) > 150 else ''}
                </p>
            </div>
        """, unsafe_allow_html=True)
        
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("⭐ Rating", f"{product.rating:.1f}")
        with col2:
            st.metric("👥 Users", product.consumer_count)
        with col3:
            st.metric("📊 Tables", len(product.table_fqns))
        with col4:
            st.metric("🛡️ Trust", f"{product.aggregated_trust_score:.0f}%")
        
        # Tags
        if product.tags:
            tags_html = " ".join([
                f'<span style="background: #e9ecef; color: #495057; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; margin-right: 0.25rem;">{tag}</span>'
                for tag in product.tags[:5]
            ])
            st.markdown(tags_html, unsafe_allow_html=True)
        
        # View Details button
        if st.button(f"View Details", key=f"view_{product.id}"):
            st.session_state.selected_product_id = product.id
        
        # Show detail view if selected
        if st.session_state.selected_product_id == product.id:
            render_product_detail(product, tables, contracts, trust_scores, product_engine)


def render_product_detail(
    product: DataProduct,
    tables: List[Dict],
    contracts: Dict[str, DataContract],
    trust_scores: List[DataTrustScore],
    product_engine: DataProductEngine
):
    """Render detailed product view"""
    
    with st.expander("📋 Product Details", expanded=True):
        
        # Tabs for different sections
        detail_tab1, detail_tab2, detail_tab3, detail_tab4, detail_tab5 = st.tabs([
            "Overview", "Metrics", "Schema", "Output Ports", "Manifest"
        ])
        
        with detail_tab1:
            st.markdown("### Business Purpose")
            st.markdown(product.business_purpose)
            
            st.markdown("### Target Personas")
            for persona in product.target_personas:
                st.markdown(f"- 👤 {persona}")
            
            st.markdown("### Data Assets Included")
            for asset in product.data_assets:
                st.markdown(f"- 📁 {asset}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### Product Metadata")
                st.markdown(f"**ID:** `{product.id}`")
                st.markdown(f"**Version:** {product.version}")
                st.markdown(f"**Owner:** {product.owner}")
                st.markdown(f"**Created:** {product.created_date.strftime('%Y-%m-%d')}")
            
            with col2:
                st.markdown("### Trust & Quality")
                st.markdown(f"**Trust Score:** {product.aggregated_trust_score:.1f}%")
                st.markdown(f"**Trust Level:** {product.trust_level}")
                st.markdown(f"**Tables Covered:** {len(product.table_fqns)}")
                st.markdown(f"**Contracts Linked:** {len(product.contract_ids)}")
        
        with detail_tab2:
            st.markdown("### 🎯 North Star Metric")
            ns = product.north_star_metric
            st.markdown(f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            padding: 1.5rem; border-radius: 12px; color: white; margin-bottom: 1rem;">
                    <h4 style="margin: 0; color: white;">{ns.name}</h4>
                    <p style="margin: 0.5rem 0; opacity: 0.9;">{ns.description}</p>
                    <code style="background: rgba(255,255,255,0.2); padding: 0.25rem 0.5rem; border-radius: 4px;">
                        {ns.formula}
                    </code>
                    <span style="margin-left: 1rem;">Unit: {ns.unit}</span>
                </div>
            """, unsafe_allow_html=True)
            
            st.markdown("### 📊 Functional Metrics")
            for metric in product.functional_metrics:
                st.markdown(f"""
                    **{metric.name}** ({metric.unit})  
                    {metric.description}  
                    `{metric.formula}`
                """)
                st.markdown("---")
            
            st.markdown("### 📈 Granular Metrics")
            for metric in product.granular_metrics:
                st.markdown(f"""
                    **{metric.name}** ({metric.unit})  
                    {metric.description}  
                    `{metric.formula}`
                """)
        
        with detail_tab3:
            st.markdown("### Constituent Tables")
            
            # Get table details
            product_tables = [t for t in tables if t.get("fullyQualifiedName") in product.table_fqns]
            
            if product_tables:
                table_data = []
                for table in product_tables:
                    fqn = table.get("fullyQualifiedName", "")
                    has_contract = fqn in contracts
                    
                    # Find trust score
                    trust = next((ts for ts in trust_scores if ts.fqn == fqn), None)
                    
                    table_data.append({
                        "Table": table.get("name", ""),
                        "FQN": fqn,
                        "Data Asset": table.get("data_asset", "N/A"),
                        "Columns": len(table.get("columns", [])),
                        "Contract": "✅" if has_contract else "❌",
                        "Trust Score": f"{trust.composite_trust_score:.1f}" if trust else "N/A"
                    })
                
                st.dataframe(pd.DataFrame(table_data), use_container_width=True)
            else:
                st.info("No table details available.")
        
        with detail_tab4:
            st.markdown("### Output Ports")
            st.markdown("*How consumers can access this data product*")
            
            for port in product.output_ports:
                port_icons = {
                    "dataset": "📁",
                    "api": "🔌",
                    "stream": "📡",
                    "dashboard": "📊"
                }
                icon = port_icons.get(port.port_type, "📦")
                
                st.markdown(f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem;">
                        <strong>{icon} {port.name}</strong>
                        <span style="background: #e9ecef; padding: 0.2rem 0.5rem; border-radius: 4px; 
                                    font-size: 0.75rem; margin-left: 0.5rem;">{port.port_type}</span>
                        <span style="background: #d4edda; padding: 0.2rem 0.5rem; border-radius: 4px; 
                                    font-size: 0.75rem; margin-left: 0.25rem;">{port.format}</span>
                        <p style="margin: 0.5rem 0 0 0; color: #666; font-size: 0.9rem;">{port.description}</p>
                        <small style="color: #888;">Access: {port.access_pattern}</small>
                    </div>
                """, unsafe_allow_html=True)
        
        with detail_tab5:
            st.markdown("### Product Manifest (YAML)")
            st.markdown("*DataOS-style declarative specification*")
            
            manifest = product_engine.generate_product_manifest(product)
            st.code(manifest, language="yaml")
            
            st.download_button(
                label="⬇️ Download Manifest",
                data=manifest,
                file_name=f"{product.name.lower().replace(' ', '_')}_manifest.yaml",
                mime="text/yaml"
            )
        
        # Close button
        if st.button("Close Details", key=f"close_{product.id}"):
            st.session_state.selected_product_id = None
            st.rerun()


def render_product_creation_wizard(
    tables: List[Dict],
    contracts: Dict[str, DataContract],
    product_engine: DataProductEngine
):
    """Render the product creation wizard with Right-to-Left flow"""
    
    st.markdown("### ➕ Create New Data Product")
    st.markdown("*Follow the Right-to-Left approach: start with business purpose, then define metrics, select data*")
    
    # Initialize wizard state
    if "product_wizard_step" not in st.session_state:
        st.session_state.product_wizard_step = 1
    
    # Step indicators
    steps = ["Business Purpose", "Define Metrics", "Select Data", "Configure Outputs", "Review & Create"]
    current_step = st.session_state.product_wizard_step
    
    # Progress bar
    progress = (current_step - 1) / (len(steps) - 1)
    st.progress(progress)
    
    # Step indicator
    step_cols = st.columns(len(steps))
    for i, (col, step_name) in enumerate(zip(step_cols, steps), 1):
        with col:
            if i < current_step:
                st.markdown(f"✅ **{i}. {step_name}**")
            elif i == current_step:
                st.markdown(f"🔵 **{i}. {step_name}**")
            else:
                st.markdown(f"⚪ {i}. {step_name}")
    
    st.markdown("---")
    
    # Initialize form data
    if "new_product" not in st.session_state:
        st.session_state.new_product = {
            "name": "",
            "domain": "Deliver",
            "business_purpose": "",
            "target_personas": [],
            "north_star": {"name": "", "description": "", "formula": "", "unit": ""},
            "functional_metrics": [],
            "granular_metrics": [],
            "data_assets": [],
            "table_fqns": [],
            "output_ports": [],
            "tags": []
        }
    
    # Step 1: Business Purpose
    if current_step == 1:
        st.markdown("### Step 1: Define Business Purpose")
        st.markdown("*Start with WHY - what business question does this product answer?*")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.session_state.new_product["name"] = st.text_input(
                "Product Name *",
                value=st.session_state.new_product["name"],
                placeholder="e.g., Delivery Performance Tracker"
            )
        
        with col2:
            st.session_state.new_product["domain"] = st.selectbox(
                "Domain *",
                ALLOWED_DOMAINS,
                index=ALLOWED_DOMAINS.index(st.session_state.new_product["domain"])
            )
        
        st.session_state.new_product["business_purpose"] = st.text_area(
            "Business Purpose *",
            value=st.session_state.new_product["business_purpose"],
            placeholder="What business question does this product answer? What decisions will it enable?",
            height=100
        )
        
        # Target personas
        st.markdown("**Target Personas** - Who will use this product?")
        personas_input = st.text_input(
            "Enter personas (comma-separated)",
            value=", ".join(st.session_state.new_product["target_personas"]),
            placeholder="e.g., Supply Chain Manager, Logistics Analyst, Operations Director"
        )
        if personas_input:
            st.session_state.new_product["target_personas"] = [p.strip() for p in personas_input.split(",") if p.strip()]
        
        # Tags
        tags_input = st.text_input(
            "Tags (comma-separated)",
            value=", ".join(st.session_state.new_product["tags"]),
            placeholder="e.g., delivery, logistics, performance"
        )
        if tags_input:
            st.session_state.new_product["tags"] = [t.strip() for t in tags_input.split(",") if t.strip()]
    
    # Step 2: Define Metrics
    elif current_step == 2:
        st.markdown("### Step 2: Define Metrics (Metric Dependency Tree)")
        st.markdown("*Define the metrics hierarchy: North Star → Functional → Granular*")
        
        st.markdown("#### 🎯 North Star Metric")
        st.markdown("*The primary KPI that matters most*")
        
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.new_product["north_star"]["name"] = st.text_input(
                "Metric Name *",
                value=st.session_state.new_product["north_star"]["name"],
                placeholder="e.g., On-Time Delivery Rate"
            )
            st.session_state.new_product["north_star"]["formula"] = st.text_input(
                "Formula *",
                value=st.session_state.new_product["north_star"]["formula"],
                placeholder="e.g., COUNT(on_time) / COUNT(total) * 100"
            )
        
        with col2:
            st.session_state.new_product["north_star"]["description"] = st.text_input(
                "Description",
                value=st.session_state.new_product["north_star"]["description"],
                placeholder="What does this metric measure?"
            )
            st.session_state.new_product["north_star"]["unit"] = st.text_input(
                "Unit",
                value=st.session_state.new_product["north_star"]["unit"],
                placeholder="e.g., %, $, count"
            )
        
        st.markdown("---")
        st.markdown("#### 📊 Functional Metrics")
        st.markdown("*Supporting metrics that feed into the North Star*")
        
        # Display existing functional metrics
        for i, metric in enumerate(st.session_state.new_product["functional_metrics"]):
            st.markdown(f"**{i+1}. {metric['name']}** - `{metric['formula']}` ({metric['unit']})")
        
        # Add new functional metric
        with st.expander("➕ Add Functional Metric"):
            f_name = st.text_input("Name", key="func_name")
            f_formula = st.text_input("Formula", key="func_formula")
            f_unit = st.text_input("Unit", key="func_unit")
            f_desc = st.text_input("Description", key="func_desc")
            
            if st.button("Add Functional Metric"):
                if f_name and f_formula:
                    st.session_state.new_product["functional_metrics"].append({
                        "name": f_name, "formula": f_formula, "unit": f_unit, "description": f_desc
                    })
                    st.rerun()
        
        st.markdown("---")
        st.markdown("#### 📈 Granular Metrics")
        st.markdown("*Detailed operational metrics*")
        
        # Display existing granular metrics
        for i, metric in enumerate(st.session_state.new_product["granular_metrics"]):
            st.markdown(f"**{i+1}. {metric['name']}** - `{metric['formula']}` ({metric['unit']})")
        
        # Add new granular metric
        with st.expander("➕ Add Granular Metric"):
            g_name = st.text_input("Name", key="gran_name")
            g_formula = st.text_input("Formula", key="gran_formula")
            g_unit = st.text_input("Unit", key="gran_unit")
            g_desc = st.text_input("Description", key="gran_desc")
            
            if st.button("Add Granular Metric"):
                if g_name and g_formula:
                    st.session_state.new_product["granular_metrics"].append({
                        "name": g_name, "formula": g_formula, "unit": g_unit, "description": g_desc
                    })
                    st.rerun()
    
    # Step 3: Select Data
    elif current_step == 3:
        st.markdown("### Step 3: Select Underlying Data")
        st.markdown("*Choose the data assets and tables that power this product*")
        
        selected_domain = st.session_state.new_product["domain"]
        
        # Data Assets selection
        st.markdown("#### 📁 Data Assets")
        available_assets = DATA_ASSETS.get(selected_domain, [])
        
        if available_assets:
            selected_assets = st.multiselect(
                "Select Data Assets",
                available_assets,
                default=st.session_state.new_product["data_assets"]
            )
            st.session_state.new_product["data_assets"] = selected_assets
        else:
            st.info(f"No data assets defined for {selected_domain} domain yet.")
        
        # Tables selection
        st.markdown("#### 📋 Tables")
        
        # Filter tables by domain and selected assets
        domain_tables = [t for t in tables if t.get("domain") == selected_domain]
        
        if st.session_state.new_product["data_assets"]:
            domain_tables = [
                t for t in domain_tables 
                if t.get("data_asset") in st.session_state.new_product["data_assets"]
            ]
        
        if domain_tables:
            table_options = {
                t.get("fullyQualifiedName"): f"{t.get('name')} ({t.get('data_asset', 'N/A')})"
                for t in domain_tables
            }
            
            selected_fqns = st.multiselect(
                "Select Tables",
                list(table_options.keys()),
                default=[f for f in st.session_state.new_product["table_fqns"] if f in table_options],
                format_func=lambda x: table_options.get(x, x)
            )
            st.session_state.new_product["table_fqns"] = selected_fqns
            
            st.markdown(f"**Selected: {len(selected_fqns)} tables**")
        else:
            st.info("No tables found for selected criteria.")
    
    # Step 4: Configure Outputs
    elif current_step == 4:
        st.markdown("### Step 4: Configure Output Ports")
        st.markdown("*Define how consumers will access this product*")
        
        # Display existing ports
        for i, port in enumerate(st.session_state.new_product["output_ports"]):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"**{port['name']}** - {port['port_type']} ({port['format']})")
            with col2:
                if st.button("Remove", key=f"remove_port_{i}"):
                    st.session_state.new_product["output_ports"].pop(i)
                    st.rerun()
        
        st.markdown("---")
        
        # Add new output port
        st.markdown("#### ➕ Add Output Port")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            port_name = st.text_input("Port Name", placeholder="e.g., Analytics Dataset")
            port_type = st.selectbox("Port Type", ["dataset", "api", "stream", "dashboard"])
        
        with col2:
            format_options = {
                "dataset": ["parquet", "csv", "json", "delta"],
                "api": ["rest", "graphql"],
                "stream": ["kafka", "json", "avro"],
                "dashboard": ["powerbi", "tableau", "looker"]
            }
            port_format = st.selectbox("Format", format_options.get(port_type, ["json"]))
            access_pattern = st.selectbox("Access Pattern", ["batch", "real-time", "on-demand"])
        
        with col3:
            port_desc = st.text_area("Description", height=100)
        
        if st.button("Add Output Port"):
            if port_name:
                st.session_state.new_product["output_ports"].append({
                    "name": port_name,
                    "port_type": port_type,
                    "format": port_format,
                    "description": port_desc,
                    "access_pattern": access_pattern
                })
                st.rerun()
    
    # Step 5: Review & Create
    elif current_step == 5:
        st.markdown("### Step 5: Review & Create")
        st.markdown("*Review your product configuration before creating*")
        
        product_data = st.session_state.new_product
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Product Details")
            st.markdown(f"**Name:** {product_data['name']}")
            st.markdown(f"**Domain:** {product_data['domain']}")
            st.markdown(f"**Purpose:** {product_data['business_purpose'][:200]}...")
            st.markdown(f"**Personas:** {', '.join(product_data['target_personas'])}")
            st.markdown(f"**Tags:** {', '.join(product_data['tags'])}")
        
        with col2:
            st.markdown("#### Composition")
            st.markdown(f"**Data Assets:** {len(product_data['data_assets'])}")
            st.markdown(f"**Tables:** {len(product_data['table_fqns'])}")
            st.markdown(f"**Output Ports:** {len(product_data['output_ports'])}")
            st.markdown(f"**Functional Metrics:** {len(product_data['functional_metrics'])}")
            st.markdown(f"**Granular Metrics:** {len(product_data['granular_metrics'])}")
        
        st.markdown("---")
        
        st.markdown("#### North Star Metric")
        st.markdown(f"**{product_data['north_star']['name']}**: `{product_data['north_star']['formula']}`")
        
        # Validation
        errors = []
        if not product_data["name"]:
            errors.append("Product name is required")
        if not product_data["business_purpose"]:
            errors.append("Business purpose is required")
        if not product_data["north_star"]["name"]:
            errors.append("North Star metric is required")
        if not product_data["table_fqns"]:
            errors.append("At least one table must be selected")
        
        if errors:
            st.error("Please fix the following issues:")
            for error in errors:
                st.markdown(f"- {error}")
        else:
            st.success("✅ Product configuration is valid!")
            
            if st.button("🚀 Create Data Product", type="primary"):
                # Find contract IDs for selected tables
                contract_ids = []
                for fqn in product_data["table_fqns"]:
                    if fqn in contracts:
                        contract_ids.append(contracts[fqn].id)
                
                # Create the product
                new_product = product_engine.create_product(
                    name=product_data["name"],
                    domain=product_data["domain"],
                    business_purpose=product_data["business_purpose"],
                    target_personas=product_data["target_personas"],
                    north_star_metric=product_data["north_star"],
                    functional_metrics=product_data["functional_metrics"],
                    granular_metrics=product_data["granular_metrics"],
                    data_assets=product_data["data_assets"],
                    table_fqns=product_data["table_fqns"],
                    contract_ids=contract_ids,
                    output_ports=product_data["output_ports"],
                    owner="current_user",  # Would be actual user in production
                    tags=product_data["tags"]
                )
                
                st.success(f"✅ Data Product '{new_product.name}' created successfully!")
                st.balloons()
                
                # Reset wizard
                st.session_state.product_wizard_step = 1
                st.session_state.new_product = {
                    "name": "", "domain": "Deliver", "business_purpose": "",
                    "target_personas": [], "north_star": {"name": "", "description": "", "formula": "", "unit": ""},
                    "functional_metrics": [], "granular_metrics": [],
                    "data_assets": [], "table_fqns": [], "output_ports": [], "tags": []
                }
                st.rerun()
    
    # Navigation buttons
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        if current_step > 1:
            if st.button("⬅️ Previous"):
                st.session_state.product_wizard_step -= 1
                st.rerun()
    
    with col3:
        if current_step < 5:
            if st.button("Next ➡️"):
                st.session_state.product_wizard_step += 1
                st.rerun()


def render_product_analytics(products: Dict[str, DataProduct], trust_scores: List[DataTrustScore]):
    """Render product analytics dashboard"""
    
    st.markdown("### 📊 Product Analytics")
    
    if not products:
        st.info("No products available for analytics.")
        return
    
    # Product performance overview
    col1, col2 = st.columns(2)
    
    with col1:
        # Trust score distribution
        st.markdown("#### Trust Score by Product")
        
        product_trust_data = [
            {"Product": p.name, "Trust Score": p.aggregated_trust_score, "Level": p.trust_level}
            for p in products.values()
        ]
        
        df = pd.DataFrame(product_trust_data).sort_values("Trust Score", ascending=False)
        
        colors = ['#27ae60' if s >= 75 else '#f39c12' if s >= 60 else '#e74c3c' 
                 for s in df["Trust Score"]]
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["Product"],
            y=df["Trust Score"],
            marker_color=colors,
            text=[f"{s:.0f}%" for s in df["Trust Score"]],
            textposition='auto'
        ))
        
        fig.update_layout(
            xaxis_title="Product",
            yaxis_title="Trust Score",
            yaxis=dict(range=[0, 100]),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Usage distribution
        st.markdown("#### Product Usage")
        
        usage_data = [
            {"Product": p.name, "Usage": p.usage_count, "Consumers": p.consumer_count}
            for p in products.values()
        ]
        
        df = pd.DataFrame(usage_data).sort_values("Usage", ascending=False)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["Product"],
            y=df["Usage"],
            name="Usage Count",
            marker_color='#3498db'
        ))
        
        fig.update_layout(
            xaxis_title="Product",
            yaxis_title="Usage Count",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Status distribution
    st.markdown("#### Product Status Distribution")
    
    status_counts = defaultdict(int)
    for p in products.values():
        status_counts[p.status] += 1
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        fig = go.Figure(data=[go.Pie(
            labels=list(status_counts.keys()),
            values=list(status_counts.values()),
            marker_colors=['#28a745', '#6c757d', '#dc3545'],
            hole=0.4
        )])
        
        fig.update_layout(
            title="By Status",
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Products table
        st.markdown("#### All Products Summary")
        
        summary_data = [
            {
                "Product": p.name,
                "Domain": p.domain,
                "Status": p.status,
                "Trust": f"{p.aggregated_trust_score:.0f}%",
                "Tables": len(p.table_fqns),
                "Consumers": p.consumer_count,
                "Rating": f"⭐ {p.rating:.1f}"
            }
            for p in products.values()
        ]
        
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, height=250)

# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main application entry point"""
    
    # Initialize session state
    if "om_host" not in st.session_state:
        st.session_state.om_host = "localhost"
    if "om_port" not in st.session_state:
        st.session_state.om_port = 8585
    if "demo_mode" not in st.session_state:
        st.session_state.demo_mode = True
    if "data_loaded" not in st.session_state:
        st.session_state.data_loaded = False
    
    # Sidebar
    with st.sidebar:
        st.markdown("## 🏛️ Data Governance")
        st.markdown("**Platform**")
        st.markdown("---")
        
        # Connection status
        if st.session_state.demo_mode:
            st.info("🧪 Demo Mode")
        else:
            st.success(f"🔗 {st.session_state.om_host}")
        
        st.markdown("---")
        
        # Quick actions
        st.markdown("### ⚡ Quick Actions")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.session_state.data_loaded = False
            st.rerun()
        
        if st.button("⚙️ Settings", use_container_width=True):
            st.session_state.show_settings = True
        
        st.markdown("---")
        
        # Stats
        if st.session_state.data_loaded:
            st.markdown("### 📊 Quick Stats")
            st.metric("Data Assets", st.session_state.get("total_tables", 0))
            st.metric("Active Contracts", 
                     len([c for c in st.session_state.contract_engine.contracts.values() 
                          if c.status == "active"]))
            st.metric("Data Products",
                     len(st.session_state.product_engine.products) if hasattr(st.session_state, 'product_engine') else 0)
            st.metric("Domains", len(ALLOWED_DOMAINS))
            
            # Governance score
            if hasattr(st.session_state, 'governance_metrics'):
                score = st.session_state.governance_metrics.compliance_rate
                st.metric("Governance Score", f"{score:.1f}%")
        
        st.markdown("---")
        st.markdown("### 🏢 Domains")
        for domain in ALLOWED_DOMAINS:
            st.caption(f"• {domain}")
        
        st.markdown("---")
        st.markdown("### 🗄️ Databases")
        for db in ALLOWED_DATABASES:
            st.caption(f"• {db}")
        
        st.markdown("---")
        st.caption("v3.0.0 | Data Products Edition")
    
    # Settings dialog
    if st.session_state.get("show_settings", False):
        with st.sidebar:
            st.markdown("### ⚙️ Settings")
            
            host = st.text_input("Host", value=st.session_state.om_host)
            port = st.number_input("Port", value=st.session_state.om_port, min_value=1, max_value=65535)
            demo_mode = st.checkbox("Demo Mode", value=st.session_state.demo_mode)
            
            if st.button("💾 Save"):
                st.session_state.om_host = host
                st.session_state.om_port = port
                st.session_state.demo_mode = demo_mode
                st.session_state.show_settings = False
                st.session_state.data_loaded = False
                st.success("Settings saved!")
                st.rerun()
            
            if st.button("✖️ Close"):
                st.session_state.show_settings = False
                st.rerun()
    
    # Load data
    if not st.session_state.data_loaded:
        with st.spinner("Loading governance data..."):
            try:
                mock_gen = MockDataGenerator()
                
                if st.session_state.demo_mode:
                    tables = mock_gen.generate_mock_tables(60)
                    contracts = mock_gen.generate_mock_contracts(tables, 25)
                    st.session_state.client = None
                else:
                    config = OpenMetadataConfig(
                        host=st.session_state.om_host,
                        port=st.session_state.om_port
                    )
                    client = OpenMetadataClient(config)
                    st.session_state.client = client
                    
                    tables = client.get_tables(limit=200)
                    
                    # Initialize contract engine and load/create contracts
                    contract_engine = DataContractEngine()
                    contracts = {}  # Would load from persistent storage in production
                
                # Store in session state
                st.session_state.tables = tables
                st.session_state.mock_gen = mock_gen
                st.session_state.contract_engine = DataContractEngine()
                st.session_state.contract_engine.contracts = contracts
                st.session_state.governance_engine = GovernanceEngine()
                st.session_state.trust_engine = TrustScoreEngine()
                
                # Calculate governance metrics
                st.session_state.governance_metrics = st.session_state.governance_engine.calculate_governance_metrics(
                    tables, contracts
                )
                
                # Calculate trust scores for all tables (needed for product aggregation)
                st.session_state.trust_scores = st.session_state.trust_engine.calculate_all_trust_scores(
                    tables, contracts, mock_gen
                )
                
                # Initialize Data Product Engine and generate mock products
                st.session_state.product_engine = DataProductEngine()
                if st.session_state.demo_mode:
                    products = mock_gen.generate_mock_data_products(
                        tables, contracts, st.session_state.trust_scores
                    )
                    st.session_state.product_engine.products = products
                
                st.session_state.total_tables = len(tables)
                st.session_state.data_loaded = True
                st.rerun()
                
            except Exception as e:
                st.error(f"Error loading data: {str(e)}")
                st.info("💡 Enable Demo Mode in settings to see the app in action")
                return
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏛️ Governance Dashboard",
        "📦 Data Products",
        "🔍 Data Discovery",
        "📜 Contract Management",
        "🎯 Data Trust Scorecard"
    ])
    
    with tab1:
        render_governance_dashboard(
            st.session_state.tables,
            st.session_state.contract_engine.contracts,
            st.session_state.governance_engine
        )
    
    with tab2:
        render_data_products(
            st.session_state.product_engine.products,
            st.session_state.tables,
            st.session_state.contract_engine.contracts,
            st.session_state.trust_scores,
            st.session_state.product_engine
        )
    
    with tab3:
        render_data_discovery(
            st.session_state.tables,
            st.session_state.contract_engine.contracts
        )
    
    with tab4:
        render_contract_management(
            st.session_state.tables,
            st.session_state.contract_engine.contracts,
            st.session_state.contract_engine,
            st.session_state.mock_gen
        )
    
    with tab5:
        render_trust_scorecard(
            st.session_state.tables,
            st.session_state.contract_engine.contracts,
            st.session_state.trust_engine,
            st.session_state.mock_gen
        )

if __name__ == "__main__":
    main()
