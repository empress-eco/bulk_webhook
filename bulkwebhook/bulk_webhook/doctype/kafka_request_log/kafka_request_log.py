# Copyright (c) 2023, Aakvatech and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import now_datetime
from frappe.model.document import Document


class KafkaRequestLog(Document):
    pass


@frappe.whitelist()
def create_kafka_request_log(doctype=None, docname=None, status=None, doc_list=None):
    """
    Create a Kafka Request Log entry for the given doc.
    args:
        doctype: doctype of the document whose kafka request log is to be created
        docname: name of the document whose kafka request log is to be created
        status: status of the kafka request
        doc_list: list of documents whose kafka request log is to be created
    """

    def create_log(doctype, docname, status, no_of_document, is_single=True):
        modified = None
        if is_single:
            modified = frappe.db.get_value(doctype, docname, "modified")
        log = frappe.get_doc(
            {
                "doctype": "Kafka Request Log",
                "ref_doctype": doctype,
                "ref_docname": docname,
                "status": status,
                "doc_modified": modified,
                "log_creation": now_datetime(),
                "no_of_document": no_of_document,
            }
        ).insert(ignore_permissions=True)

    if doc_list and len(doc_list) > 1:
        for doc in doc_list:
            create_log(doctype, docname, status, 1)

        no_of_document = len(doc_list)
        create_log(doctype, docname, status, no_of_document, is_single=False)
    elif doctype and docname:
        create_log(doctype, docname, status, 1)

    frappe.db.commit()
