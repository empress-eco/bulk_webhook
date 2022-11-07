from __future__ import unicode_literals
import json
import frappe
from frappe import _
from frappe.utils.background_jobs import enqueue

@frappe.whitelist()
def resend_kafkahook_for_docs(args):
    """
    API Path: <URL>://kyosk.api.api.resend_kafkahook_for_docs
    args incloudes:
    kafkahook_name: Document Name of the kafkahook to fire. e.g. "HOOK-0016"
    doctype_name: Document Type Name to fire the kafkahook on. e.g. "Sales Order"
    doc_list: list - List of Names of documents for which to fire the kafkahook. e.g. ["SAL-ORD-JUJ001", "SAL-ORD-JUJ002"]
    """
    kafkahook_name = args.get("kafkahook_name")
    if not kafkahook_name:
        frappe.throw("kafkahook Name is required")
    doctype_name = args.get("doctype_name")
    if not doctype_name:
        frappe.throw("Doctype Name is required")
    doc_list = args.get("doc_list")
    if not doc_list:
        frappe.throw("Doc List is required")
    kafkahook = {}
    kafkahook["name"] = kafkahook_name
    from bulkwebhook.bulk_webhook.doctype.kafka_hook.kafka_hook import enqueue_webhook

    for doc_name in doc_list:
        doc = frappe.get_doc(doctype_name, doc_name)
        enqueue(
            method=enqueue_webhook,
            queue="long",
            timeout=10000,
            is_async=True,
            doc=doc,
            kafkahook=kafkahook,
        )
    return "KafkaHook sending is schdeduled"
