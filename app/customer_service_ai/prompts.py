from __future__ import annotations

CLASSIFICATION_PROMPT = """You are an AI assistant that analyzes Amazon buyer messages.

Your task is to classify the buyer message.

Categories:

shipping
tracking
delivery_delay
product_usage
assembly
missing_parts
damage
defective
return_refund
replacement
complaint
angry_customer
order_cancel
other

Buyer message:
{buyer_message}

Return JSON:

{
"category": "",
"confidence": ""
}
"""

SENTIMENT_PROMPT = """Analyze the emotional tone of the buyer message.

Sentiment options:

positive
neutral
negative
angry

Buyer message:
{buyer_message}

Return JSON:

{
"sentiment": "",
"confidence": ""
}
"""

RISK_DETECTION_PROMPT = """Analyze the risk level of this Amazon buyer message.

Risk levels:

low
medium
high

High risk examples:

angry buyers

product damage

refund threats

complaints about quality

Buyer message:
{buyer_message}

Return JSON:

{
"risk_level": "",
"reason": ""
}
"""

PRODUCT_ISSUE_PROMPT = """Extract the product issue mentioned by the buyer.

Examples:

broken leg
missing screw
fabric torn
difficult assembly

Buyer message:
{buyer_message}

Return JSON:

{
"product_issue": ""
}
"""

SHIPPING_REPLY_PROMPT = """You are a professional Amazon seller customer support agent.

The buyer is asking about shipping or delivery.

Instructions:

reassure the buyer

suggest checking tracking information

offer further help if necessary

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

DELIVERY_DELAY_REPLY_PROMPT = """The buyer reports that the order has not arrived yet.

Instructions:

apologize for the delay

reassure the buyer

suggest checking tracking

offer further help

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

PRODUCT_USAGE_REPLY_PROMPT = """The buyer is asking how to use the product.

Instructions:

give clear guidance

keep it simple

offer further help

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

ASSEMBLY_REPLY_PROMPT = """The buyer needs help assembling the product.

Instructions:

provide simple step guidance

be patient and supportive

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

MISSING_PARTS_REPLY_PROMPT = """The buyer reports missing parts.

Instructions:

apologize sincerely

offer replacement parts

reassure the buyer

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

DAMAGE_REPLY_PROMPT = """The buyer reports that the product arrived damaged.

Instructions:

apologize sincerely

show empathy

offer replacement or solution

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

DEFECTIVE_REPLY_PROMPT = """The buyer reports that the product is defective.

Instructions:

apologize for the issue

offer troubleshooting or replacement

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

RETURN_REFUND_REPLY_PROMPT = """The buyer wants a return or refund.

Instructions:

explain that returns can be initiated via Amazon orders

offer assistance

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

COMPLAINT_REPLY_PROMPT = """The buyer is dissatisfied.

Instructions:

apologize sincerely

show empathy

offer a solution

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

ANGRY_CUSTOMER_REPLY_PROMPT = """The buyer is clearly angry.

Instructions:

stay calm

acknowledge the frustration

apologize sincerely

offer resolution

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

ORDER_CANCEL_REPLY_PROMPT = """The buyer wants to cancel the order.

Instructions:

explain the cancellation process

respond politely

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

FALLBACK_REPLY_PROMPT = """The buyer message does not clearly match any category.

Instructions:

respond politely

ask for clarification

Buyer message:
{buyer_message}

Return JSON:

{
"reply": ""
}
"""

CATEGORY_OPTIONS = {
    "shipping",
    "tracking",
    "delivery_delay",
    "product_usage",
    "assembly",
    "missing_parts",
    "damage",
    "defective",
    "return_refund",
    "replacement",
    "complaint",
    "angry_customer",
    "order_cancel",
    "other",
}

SENTIMENT_OPTIONS = {"positive", "neutral", "negative", "angry"}
RISK_OPTIONS = {"low", "medium", "high"}

REPLY_PROMPT_BY_CATEGORY = {
    "shipping": SHIPPING_REPLY_PROMPT,
    "tracking": SHIPPING_REPLY_PROMPT,
    "delivery_delay": DELIVERY_DELAY_REPLY_PROMPT,
    "product_usage": PRODUCT_USAGE_REPLY_PROMPT,
    "assembly": ASSEMBLY_REPLY_PROMPT,
    "missing_parts": MISSING_PARTS_REPLY_PROMPT,
    "damage": DAMAGE_REPLY_PROMPT,
    "defective": DEFECTIVE_REPLY_PROMPT,
    "return_refund": RETURN_REFUND_REPLY_PROMPT,
    "replacement": DEFECTIVE_REPLY_PROMPT,
    "complaint": COMPLAINT_REPLY_PROMPT,
    "angry_customer": ANGRY_CUSTOMER_REPLY_PROMPT,
    "order_cancel": ORDER_CANCEL_REPLY_PROMPT,
    "other": FALLBACK_REPLY_PROMPT,
}
