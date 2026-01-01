from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class OrderTable(Base):
    __tablename__ = 'raw_orders'

    order_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    website_session_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    primary_product_id = Column(Integer, nullable=False)
    items_purchased = Column(Integer, nullable=False)
    price_usd = Column(Float, nullable=False)
    cogs_usd = Column(Float, nullable=False)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<order_id={self.order_id}>'
    
class OrderItemTable(Base):
    __tablename__ = 'raw_order_items'

    order_item_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    order_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)
    is_primary_item = Column(Integer, nullable=False)
    price_usd = Column(Float, nullable=False)
    cogs_usd = Column(Float, nullable=False)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<order_item_id={self.order_item_id}>'
    

class OrderItemRefundTable(Base):
    __tablename__ = 'raw_order_item_refund'

    order_item_refund_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    order_item_id = Column(Integer, nullable=False)
    order_id = Column(Integer, nullable=False)
    refund_amount_usd = Column(Float, nullable=False)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<order_item_refund_id={self.order_item_refund_id}>'
    
class ProductsTable(Base):
    __tablename__ = 'raw_products'

    product_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    product_name = Column(String(250), nullable=False)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<produt_id={self.product_id} | product_name={self.product_name}>'
    
class WebSiteSessionsTable(Base):
    __tablename__ = 'raw_website_sessions'

    website_session_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    user_id = Column(Integer, nullable=False)
    is_repeat_session = Column(Integer, nullable=False)
    utm_source = Column(String(250), nullable=True)
    utm_campaign = Column(String(250), nullable=True)
    utm_content = Column(String(250), nullable=True)
    device_type = Column(String(100), nullable=False)
    http_referer = Column(String(250), nullable=True)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<website_session_id={self.website_session_id}>'
    
class WebSitePageViewsTable(Base):
    __tablename__ = 'raw_website_pageviews'

    website_pageview_id = Column(Integer, nullable=False, unique=True, index=True, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    website_session_id = Column(Integer, nullable=False)
    pageview_url = Column(String(250), nullable=False)
    inserted_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<website_pageview_id={self.website_pageview_id}>'