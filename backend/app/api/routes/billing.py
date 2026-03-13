from __future__ import annotations

from datetime import datetime

import stripe
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.db.models import AccountTier, User
from app.db.session import get_db

router = APIRouter()


def _ensure_stripe():
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="stripe_not_configured")
    stripe.api_key = settings.STRIPE_SECRET_KEY


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------


@router.post("/create-checkout-session")
def create_checkout_session(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Create a Stripe Checkout session for Pro subscription."""
    _ensure_stripe()

    user = db.query(User).filter(User.id == owner_id).first()
    if not user:
        user = User(id=owner_id, email=f"{owner_id}@scout.local", tier=AccountTier.unlocked)
        db.add(user)
        db.commit()
        db.refresh(user)

    if user.tier in (AccountTier.pro, AccountTier.unlocked):
        raise HTTPException(status_code=400, detail="already_upgraded")

    if not settings.STRIPE_PRICE_ID:
        raise HTTPException(status_code=500, detail="stripe_price_not_configured")

    # Get or create Stripe customer
    customer_id = user.stripe_customer_id
    if not customer_id:
        customer = stripe.Customer.create(
            email=user.email,
            metadata={"user_id": user.id},
        )
        customer_id = customer.id
        user.stripe_customer_id = customer_id
        db.add(user)
        db.commit()

    session = stripe.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        line_items=[{"price": settings.STRIPE_PRICE_ID, "quantity": 1}],
        mode="subscription",
        success_url=f"{settings.FRONTEND_URL}/settings?checkout=success",
        cancel_url=f"{settings.FRONTEND_URL}/settings?checkout=cancel",
        metadata={"user_id": user.id},
    )
    return {"checkout_url": session.url}


# ---------------------------------------------------------------------------
# Customer Portal (manage / cancel subscription)
# ---------------------------------------------------------------------------


@router.post("/create-portal-session")
def create_portal_session(
    owner_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Create a Stripe Customer Portal session."""
    _ensure_stripe()

    user = db.query(User).filter(User.id == owner_id).first()
    if not user or not user.stripe_customer_id:
        raise HTTPException(status_code=400, detail="no_subscription_found")

    session = stripe.billing_portal.Session.create(
        customer=user.stripe_customer_id,
        return_url=f"{settings.FRONTEND_URL}/settings",
    )
    return {"portal_url": session.url}


# ---------------------------------------------------------------------------
# Stripe Webhook
# ---------------------------------------------------------------------------


@router.post("/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle Stripe webhook events (exempt from require_admin)."""
    _ensure_stripe()

    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    if not settings.STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="webhook_secret_not_configured")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig, settings.STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="invalid_signature")

    if event.type == "checkout.session.completed":
        session_obj = event.data.object
        customer_id = session_obj.get("customer")
        subscription_id = session_obj.get("subscription")

        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user and subscription_id:
            sub = stripe.Subscription.retrieve(subscription_id)
            user.tier = AccountTier.pro
            user.stripe_subscription_id = sub.id
            user.stripe_subscription_status = sub.status
            if sub.current_period_end:
                user.stripe_current_period_end = datetime.utcfromtimestamp(
                    sub.current_period_end
                )
            db.add(user)
            db.commit()

    elif event.type in (
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        sub_obj = event.data.object
        customer_id = sub_obj.get("customer")

        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            status = sub_obj.get("status", "")
            user.stripe_subscription_status = status

            if status in ("canceled", "unpaid", "incomplete_expired"):
                user.tier = AccountTier.free
            elif status == "active":
                user.tier = AccountTier.pro

            period_end = sub_obj.get("current_period_end")
            if period_end:
                user.stripe_current_period_end = datetime.utcfromtimestamp(period_end)

            db.add(user)
            db.commit()

    return {"received": True}
