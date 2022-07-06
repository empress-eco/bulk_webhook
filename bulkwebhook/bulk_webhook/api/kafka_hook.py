# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

import frappe
from frappe import _


@frappe.whitelist()
def resend_single_kafkahook(doctype, doc_name, kafkahook_name=None):
    """
    API Path: <URL>://bulkwebhook.bulk_webhook.api.kafka_hook.resend_single_kafkahook
    doctype: Data - Document Type Name to fire the Kafka Hook on. e.g. "Sales Order"
    doc_name: Data - Name of document for which to fire the Kafka Hook. e.g. "SAL-ORD-JUJ002"
    kafkahook_name (Optional): Document Name of the Kafka Hook to fire. e.g. "HOOK-0016"
    """
    if not kafkahook_name:
        kafkahook_name = frappe.get_value(
            "Kafka Hook",
            {"webhook_doctype": doctype, "enabled": 1, "condition": ""},
            "name",
        )
    if not kafkahook_name:
        frappe.throw(
            _("Please set a webhook in the Setup > Webhooks with blank condition")
        )
    resend_kafkahook(kafkahook_name, doctype, [doc_name])


def resend_kafkahook(kafkahook_name, doctype_name, doc_list):
    """
    API Path: <URL>://bulkwebhook.bulk_webhook.api.kafka_hook.resend_kafkahook
    kafkahook_name: Document Name of the Kafka Hook to fire. e.g. "HOOK-0016"
    doctype_name: Document Type Name to fire the Kafka Hook on. e.g. "Sales Order"
    doc_list: list - List of Names of documents for which to fire the Kafka Hook. e.g. ["SAL-ORD-JUJ001", "SAL-ORD-JUJ002"]
    """
    kafkahook = {}
    kafkahook["name"] = kafkahook_name
    from bulkwebhook.bulk_webhook.doctype.kafka_hook.kafka_hook import enqueue_webhook

    for doc_name in doc_list:
        try:
            doc = frappe.get_doc(doctype_name, doc_name)
            try:
                enqueue_webhook(doc, kafkahook)
                frappe.msgprint("Webhook sent successfully")
            except Exception as e:
                frappe.log_error(str(e), "Manual Webhook enqueue")
        except Exception as e:
            frappe.msgprint("Document {0} not found".format(doc_name))
            frappe.log_error(str(e), "Manual Webhook loop")
    frappe.db.commit()


@frappe.whitelist()
def resend_kafkahook_for_docs(args):
    """
    API Path: <URL>://bulkwebhook.bulk_webhook.api.kafka_hook.resend_kafkahook_for_docs
    args incloudes:
    kafkahook_name: Document Name of the kafkahook to fire. e.g. "HOOK-0016"
    doctype_name: Document Type Name to fire the kafkahook on. e.g. "Sales Order"
    doc_list: list - List of Names of documents for which to fire the kafkahook. e.g. ["SAL-ORD-JUJ001", "SAL-ORD-JUJ002"]
    """
    kafkahook_name = args.get("kafkahook_name")
    if not kafkahook_name:
        frappe.throw("Webhook Name is required")
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
        enqueue_webhook(doc, kafkahook)
    frappe.db.commit()
    return "Webhook sent successfully"
