# Copyright (c) 2021, Aakvatech and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from bulkwebhook.bulk_webhook.doctype.bulk_webhook.bulk_webhook import enqueue_bulk_webhooks


Every_5_minutes = "Every 5 minutes"
Every_15_minutes = "Every 15 minutes"
Every_30_minutes = "Every 30 minutes"
Hourly = "Hourly"
Daily = "Daily"
Weekly = "Weekly"
Monthly = "Monthly"


def handle_5():
    enqueue_bulk_webhooks(Every_5_minutes)


def handle_15():
    enqueue_bulk_webhooks(Every_15_minutes)


def handle_30():
    enqueue_bulk_webhooks(Every_30_minutes)


def handle_hourly():
    enqueue_bulk_webhooks(Hourly)


def handle_daily():
    enqueue_bulk_webhooks(Daily)


def handle_weekly():
    enqueue_bulk_webhooks(Weekly)


def handle_monthly():
    enqueue_bulk_webhooks(Monthly)
