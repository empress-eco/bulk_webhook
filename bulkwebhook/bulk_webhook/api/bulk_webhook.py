# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

import frappe
from bulkwebhook.bulk_webhook.doctype.bulk_webhook.bulk_webhook import (
    enqueue_bulk_webhook,
)


@frappe.whitelist()
def resend_bulk_webhook(bulk_webhook_name, method_parameters=None, report_filters=None):
    """
    API Path: <URL>://bulkwebhook.bulk_webhook.api.bulk_webhook.resend_bulk_webhook
    bulk_webhook_name: Document Name of the Bulk webhook to fire. e.g. "HOOK-0016"
    method_parameters: pass deferent parameters to the method. e.g. {"doc_name": "SAL-ORD-JUJ001"}
    report_filters: pass deferent filters to the report. e.g. {"doc_name": "SAL-ORD-JUJ001"}
    """
    return enqueue_bulk_webhook(bulk_webhook_name, method_parameters, report_filters)
