# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

import frappe
import bulkwebhook
from frappe.model.document import Document


class KafkaSettings(Document):
    def clear_cache(self):
        if frappe.local.site in bulkwebhook.PRODUCER_MAP:
            bulkwebhook.PRODUCER_MAP[frappe.local.site].pop(self.name, None)
        return super().clear_cache()
