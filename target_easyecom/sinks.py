"""Easyecom target sink class, which handles writing streams."""

from __future__ import annotations

from __future__ import annotations
import json



from target_easyecom.client import EasyecomSink

class BuyOrdersSink(EasyecomSink):
    """Easyecom target sink class."""

    
    endpoint = "/WMS/Cart/CreatePurchaseOrder"
    name = "BuyOrders"
    

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        buy_order = {
            "createOrUpdate": "I",
            "isCancel": 0,
            "updateTaxRate": 1,
        }
        if record.get("supplier_remoteId"):
            buy_order["vendorId"] = record.get("supplier_remoteId")
        if record.get("externalid"):
            buy_order["referenceCode"] = record.get("externalid")
        if record.get("billing_address"):
            buy_order["address"] = json.dumps(record.get("billing_address"))
        line_items = []
        if record.get("line_items"):
            record_line_items = record.get("line_items")
            if isinstance(record_line_items, str):
                record_line_items = json.loads(record_line_items)
            line_item_number = 0
            for item in record_line_items:
                line_item = {}
                if item.get("sku"):
                    line_item["sku"] = item.get("sku")
                if item.get("quantity"):
                    line_item["quantity"] = item.get("quantity")
                if "unit_price" in item:
                    line_item["unitPrice"] = item["unit_price"]
                if item.get("tax_amount"):
                    line_item["taxValue"] = item.get("tax_amount")
                if line_item:
                    line_item_number += 1
                    line_item["lineItemNumber"] = line_item_number
                    line_items.append(line_item)
            buy_order["items"] = line_items
        return buy_order
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            if record.get("error"):
                raise Exception(record.get("error"))

            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = response.json()
            # old format found in the initial commit for this target. keeping it for backcompatibility
            links = res_json.get("_links", {}).get("self", {}).get("href", "")
            if links:
                id = links.split("/")[-1]
            else:
                # new format found
                id = res_json.get("data", {}).get("poId")

            if not id:
                raise Exception(f"Response in a different format. response={res_json}")

            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates
        
