# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

import frappe
import bulkwebhook
from frappe.model.document import Document


class KafkaSettings(Document):
    def on_update(self):
        if frappe.local.site in bulkwebhook.PRODUCER_MAP:
            bulkwebhook.PRODUCER_MAP[frappe.local.site].pop(self.name, None)
