"""
Maliyet Sistemi - Pydantic Modelleri
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal, Any
from datetime import datetime


class ProductResponse(BaseModel):
    id: int
    kategori: str
    parent_id: Optional[float] = None
    parent_name: Optional[str] = None
    child_sku: str
    child_name: Optional[str] = None
    child_code: Optional[str] = None
    child_dims: Optional[str] = None
    en: Optional[float] = None
    boy: Optional[float] = None
    alan_m2: Optional[float] = None
    variation_size: Optional[str] = None
    variation_color: Optional[str] = None
    product_identifier: Optional[str] = None
    kargo_kodu: Optional[str] = None
    kargo_en: Optional[float] = None
    kargo_boy: Optional[float] = None
    kargo_yukseklik: Optional[float] = None
    kargo_agirlik: Optional[float] = None
    kargo_desi: Optional[float] = None


class RawMaterialResponse(BaseModel):
    id: int
    name: str
    unit: str
    unit_price: float
    currency: str


class RawMaterialUpdate(BaseModel):
    unit_price: float
    currency: str = "TRY"


class RawMaterialCreate(BaseModel):
    name: str
    unit: str
    unit_price: float = 0
    currency: str = "TRY"


class ProductMaterialEntry(BaseModel):
    child_sku: str
    material_id: int
    quantity: float


class ProductMaterialBulk(BaseModel):
    """Bir ürün grubu için toplu hammadde girişi"""
    child_skus: list[str]
    material_id: int
    quantity: float


class ProductCostAssignment(BaseModel):
    child_sku: str
    cost_name: str
    assigned: bool = True


class CostDefinitionCreate(BaseModel):
    name: str
    category: Literal["kargo", "kaplama"] = "kaplama"
    kargo_code: Optional[str] = None
    is_active: bool = True


class CostDefinitionUpdate(BaseModel):
    name: Optional[str] = None
    category: Optional[Literal["kargo", "kaplama"]] = None
    kargo_code: Optional[str] = None
    is_active: Optional[bool] = None


class ExportRequest(BaseModel):
    """Excel export isteği"""
    child_skus: list[str]
    include_materials: bool = True
    include_costs: bool = True


class ParentInheritanceRequest(BaseModel):
    """Parent-to-Child Cost Inheritance isteği"""
    parent_name: str
    cost_map: dict[str, str]  # { variation_size: cost_name }
    kaplama_map: dict[str, str | list[str]] = Field(default_factory=dict)  # { variation_size: kaplama_cost_name | [kaplama_cost_names] }
    kaplama_name_map: dict[str, str | list[str]] = Field(default_factory=dict)  # { child_name||tier: kaplama_cost_name | [kaplama_cost_names] } (legacy: child_name)
    allow_missing_kaplama: bool = False  # true ise kaplama eşleşmeyen child'lar kaplama atanmadan devam eder
    weight_map: dict[str, float]  # { variation_size: kargo_agirlik }
    materials: dict[int, float]  # { material_id: quantity }
    sac_material_id: Optional[int] = None  # Seçilen Saç kalınlığı (id), miktar = Alan
    mdf_material_id: Optional[int] = None  # Seçilen MDF (id), miktar = Alan
    approval_id: Optional[int] = None  # Onay workflow aktifken approved isteği execute etmek için


class ProductSyncRequest(BaseModel):
    categories: list[str] = Field(default_factory=list)  # boş ise tüm kategoriler
    replace_existing: bool = True  # true ise seçili kategorilerdeki ürünleri yeniden oluşturur


class ApprovalReviewRequest(BaseModel):
    approve: bool = True
    review_note: Optional[str] = None


class AuthLoginRequest(BaseModel):
    username: str
    password: str


class AuthChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class AuthUserCreate(BaseModel):
    username: str
    password: str
    role: Literal["admin", "user"] = "user"
    is_active: bool = True


class AuthUserUpdate(BaseModel):
    password: Optional[str] = None
    role: Optional[Literal["admin", "user"]] = None
    is_active: Optional[bool] = None


class ParentSearchItem(BaseModel):
    parent_id: float
    parent_name: str
    parent_sku: Optional[str] = None
    child_count: int


class CostPropagationRequest(BaseModel):
    parent_id: float
    parent_name: Optional[str] = None
    parent_sku: Optional[str] = None
    cost_breakdown: dict[str, Any] = Field(default_factory=dict)


class StatsResponse(BaseModel):
    total_products: int
    metal_products: int
    ahsap_products: int
    cam_products: int
    harita_products: int
    mobilya_products: int
    products_with_dims: int
    products_without_dims: int
    total_materials: int
    materials_with_price: int
