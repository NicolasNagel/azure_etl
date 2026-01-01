import pandera.pandas as pa

from datetime import datetime

from pandera.typing import Series

class OrderSchema(pa.DataFrameModel):
    order_id: Series[int] = pa.Field(unique=True, ge=0, nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    website_session_id: Series[int] = pa.Field(ge=1, nullable=False)
    user_id: Series[int] = pa.Field(ge=1, nullable=False)
    primary_product_id: Series[int] = pa.Field(isin=[1, 2, 3, 4], nullable=False)
    items_purchased: Series[int] = pa.Field(ge=0, nullable=False)
    price_usd: Series[float] = pa.Field(gt=0, nullable=False)
    cogs_usd: Series[float] = pa.Field(gt=0, nullable=False)

    class Config:
        strict = True
        coerce = True

class OrderItemSchema(pa.DataFrameModel):
    order_item_id: Series[int] = pa.Field(unique=True, ge=0, nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    order_id: Series[int] = pa.Field(ge=0, nullable=False)
    product_id: Series[int] = pa.Field(isin=[1, 2, 3, 4], nullable=False)
    is_primary_item: Series[int] = pa.Field(isin=[0, 1], nullable=False)
    price_usd: Series[float] = pa.Field(gt=0, nullable=False)
    cogs_usd: Series[float] = pa.Field(gt=0, nullable=False)

    class Config:
        strict = True
        coerce = True

class OrderItemRefundSchema(pa.DataFrameModel):
    order_item_refund_id: Series[int] = pa.Field(unique=True, ge=0, nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    order_item_id: Series[int] = pa.Field(ge=0, nullable=False)
    order_id: Series[int] = pa.Field(ge=0, nullable=False)
    refund_amount_usd: Series[float] = pa.Field(gt=0, nullable=False)

    class Config:
        strict = True
        coerce = True

class ProductSchema(pa.DataFrameModel):
    product_id: Series[int] = pa.Field(unique=True, isin=[1, 2, 3, 4], nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    product_name: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = True
        coerce = True

class WebsiteSessionsSchema(pa.DataFrameModel):
    website_session_id: Series[int] = pa.Field(unique=True, ge=0, nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    user_id: Series[int] = pa.Field(ge=1, nullable=False)
    is_repeat_session: Series[int] = pa.Field(ge=0, nullable=False)
    utm_source: Series[str] = pa.Field(nullable=True)
    utm_campaign: Series[str] = pa.Field(nullable=True)
    utm_content: Series[str] = pa.Field(nullable=True)
    device_type: Series[str] = pa.Field(isin=['mobile', 'desktop'], nullable=True)
    http_referer: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True

class WebsitePageviewSchema(pa.DataFrameModel):
    website_pageview_id: Series[int] = pa.Field(unique=True, ge=0, nullable=False)
    created_at: Series[datetime] = pa.Field(nullable=False)
    website_session_id: Series[int] = pa.Field(ge=0, nullable=False)
    pageview_url: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True