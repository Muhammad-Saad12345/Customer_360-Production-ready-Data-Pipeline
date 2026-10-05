import unittest

from src.processing.hubspot_processor import HubSpotDataProcessor


class HubSpotProcessingTestCase(unittest.TestCase):
    def test_processor_standardizes_hubspot_contact_records(self):
        processor = HubSpotDataProcessor()
        raw_records = [
            {
                "id": "contact_001",
                "properties": {
                    "firstname": "Saad",
                    "lastname": "Khan",
                    "email": "saad@example.com",
                    "phone": "+923001234567",
                    "lifecyclestage": "customer",
                    "createdate": "2026-01-01T00:00:00Z",
                    "lastmodifieddate": "2026-01-02T00:00:00Z",
                },
            }
        ]

        processed = processor.process_contacts(raw_records)
        self.assertEqual(processed[0]["customer_id"], "contact_001")
        self.assertEqual(processed[0]["first_name"], "Saad")
        self.assertEqual(processed[0]["email"], "saad@example.com")
        self.assertEqual(processed[0]["customer_stage"], "customer")

    def test_processor_standardizes_hubspot_order_records(self):
        processor = HubSpotDataProcessor()
        raw_records = [
            {
                "id": "order_001",
                "properties": {
                    "hs_object_id": "order_001",
                    "hs_createdate": "2026-01-01T00:00:00Z",
                    "lastmodifieddate": "2026-01-02T00:00:00Z",
                    "order_status": "completed",
                    "subtotal": "150.00",
                    "tax": "12.00",
                    "shipping": "10.00",
                    "discount": "0.00",
                    "net_total": "172.00",
                    "currency": "USD",
                    "channel": "web",
                },
                "associations": {"contacts": {"results": [{"id": "contact_001"}]}}
            }
        ]

        processed = processor.process_orders(raw_records)
        self.assertEqual(processed[0]["order_id"], "order_001")
        self.assertEqual(processed[0]["customer_id"], "contact_001")
        self.assertEqual(processed[0]["net_total"], 172.0)
        self.assertEqual(processed[0]["order_status"], "completed")


if __name__ == "__main__":
    unittest.main()
