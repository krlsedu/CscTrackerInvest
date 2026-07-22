import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from service.investment_handler import InvestmentHandler


class TestInvestmentHandlerProfitLoss(unittest.TestCase):
    def setUp(self):
        self.fii_handler = MagicMock()
        self.stock_handler = MagicMock()
        self.fixed_income_handler = MagicMock()
        self.dividend_handler = MagicMock()
        self.remote_repository = MagicMock()
        self.http_repository = MagicMock()

        self.handler = InvestmentHandler(
            fii_handler=self.fii_handler,
            stock_handler=self.stock_handler,
            fixed_income_handler=self.fixed_income_handler,
            dividend_handler=self.dividend_handler,
            remote_repository=self.remote_repository,
            http_repository=self.http_repository,
        )

    @patch("service.investment_handler.RequestInfo.get_correlation_id")
    def test_add_profit_loss_positive_profit(self, mock_get_correlation_id):
        mock_get_correlation_id.return_value = "mock-req-123"
        # Setup mocks
        movement = {
            "id": 1001,
            "user_id": 1,
            "investment_id": 42,
            "quantity": 10.0,
            "price": 50.0,
            "date": "2026-07-21",
        }
        headers = {"userName": "test_user"}

        stock_mock = {"id": 42, "ticker": "AAPL", "name": "Apple Inc."}

        inserted_pl_mock = {
            "id": 5001,
            "user_id": 1,
            "investment_id": 42,
            "value": 15.0,
            "quantity": 10.0,
            "date_sell": "2026-07-21",
            "request_id": "mock-req-123",
        }

        # We need mock responses for get_object and insert
        def get_object_side_effect(table, keys, data, hdrs):
            if table == "stocks" and data.get("id") == 42:
                return stock_mock
            return None

        self.remote_repository.get_object.side_effect = get_object_side_effect

        def insert_side_effect(table, data, hdrs):
            if table == "profit_loss":
                return inserted_pl_mock
            return data

        self.remote_repository.insert.side_effect = insert_side_effect

        # Call method
        self.handler.add_profit_loss(15.0, movement, headers)

        # Assertions
        # 1. profit_loss insertion
        self.remote_repository.insert.assert_any_call(
            "profit_loss",
            {
                "user_id": 1,
                "investment_id": 42,
                "value": 15.0,
                "quantity": 10.0,
                "date_sell": "2026-07-21",
                "request_id": "mock-req-123",
            },
            headers,
        )

        # 2. Resgate transaction insertion
        # resgate_value = round((qty * price) - (pl_qty * pl_val), 2)
        # resgate_value = round((10.0 * 50.0) - (10.0 * 15.0), 2) = 500 - 150 = 350.0
        expected_resgate_tx = {
            "date": "2026-07-21",
            "type": "income",
            "value": 350.0,
            "name": "AAPL",
            "package_name": None,
            "app_name": "Nubank",
            "text": "Resgate de 10 cotas de Apple Inc. no preço de venda de 50 Reais",
            "user_id": 1,
            "last_update": "2026-07-21",
            "category": "Resgate",
            "key": "AAPL_2026-07-21_10.0_42_mock-req-123",
            "copy": False,
            "is_installment": "N",
            "installment_id": None,
            "request_id": "mock-req-123",
        }
        self.remote_repository.insert.assert_any_call(
            "transactions", expected_resgate_tx, headers
        )

        # 3. Lucro transaction insertion (profit_loss_value > 0)
        # profit_value = round(pl_qty * pl_val, 4) = round(10.0 * 15.0, 4) = 150.0
        expected_lucro_tx = {
            "date": "2026-07-21",
            "type": "income",
            "value": 150.0,
            "name": "AAPL",
            "package_name": None,
            "app_name": "Nubank",
            "text": "Lucro referente a venda de 10 cotas de Apple Inc. no preço de venda de 50 Reais",
            "user_id": 1,
            "last_update": "2026-07-21",
            "category": "Lucro investimentos",
            "key": "AAPL_2026-07-21_10.0_42_mock-req-123",
            "copy": False,
            "request_id": "mock-req-123",
            "is_installment": "N",
            "installment_id": None,
        }
        self.remote_repository.insert.assert_any_call(
            "transactions", expected_lucro_tx, headers
        )

    @patch("service.investment_handler.RequestInfo.get_correlation_id")
    def test_add_profit_loss_negative_profit(self, mock_get_correlation_id):
        mock_get_correlation_id.return_value = "mock-req-123"
        # Setup mocks
        movement = {
            "id": 1002,
            "user_id": 1,
            "investment_id": 42,
            "quantity": 5.0,
            "price": 30.0,
            "date": "2026-07-21",
        }
        headers = {"userName": "test_user"}

        stock_mock = {"id": 42, "ticker": "AAPL", "name": "Apple Inc."}

        inserted_pl_mock = {
            "id": 5002,
            "user_id": 1,
            "investment_id": 42,
            "value": -10.0,
            "quantity": 5.0,
            "date_sell": "2026-07-21",
            "request_id": "mock-req-123",
        }

        def get_object_side_effect(table, keys, data, hdrs):
            if table == "stocks" and data.get("id") == 42:
                return stock_mock
            return None

        self.remote_repository.get_object.side_effect = get_object_side_effect

        def insert_side_effect(table, data, hdrs):
            if table == "profit_loss":
                return inserted_pl_mock
            return data

        self.remote_repository.insert.side_effect = insert_side_effect

        # Call method
        self.handler.add_profit_loss(-10.0, movement, headers)

        # Assertions
        # 1. profit_loss insertion
        self.remote_repository.insert.assert_any_call(
            "profit_loss",
            {
                "user_id": 1,
                "investment_id": 42,
                "value": -10.0,
                "quantity": 5.0,
                "date_sell": "2026-07-21",
                "request_id": "mock-req-123",
            },
            headers,
        )

        # 2. Resgate transaction insertion
        # resgate_value = round((qty * price) - (pl_qty * pl_val), 2)
        # resgate_value = round((5.0 * 30.0) - (5.0 * -10.0), 2) = 150 - (-50) = 200.0
        expected_resgate_tx = {
            "date": "2026-07-21",
            "type": "income",
            "value": 200.0,
            "name": "AAPL",
            "package_name": None,
            "app_name": "Nubank",
            "text": "Resgate de 5 cotas de Apple Inc. no preço de venda de 30 Reais",
            "user_id": 1,
            "last_update": "2026-07-21",
            "category": "Resgate",
            "key": "AAPL_2026-07-21_5.0_42_mock-req-123",
            "copy": False,
            "is_installment": "N",
            "installment_id": None,
            "request_id": "mock-req-123",
        }
        self.remote_repository.insert.assert_any_call(
            "transactions", expected_resgate_tx, headers
        )

        # 3. Prejuízo transaction insertion (profit_loss_value < 0)
        # loss_value = round(pl_qty * pl_val, 4) * -1 = round(5.0 * -10.0, 4) * -1 = -50.0 * -1 = 50.0
        expected_prejuizo_tx = {
            "date": "2026-07-21",
            "type": "outcome",
            "value": 50.0,
            "name": "AAPL",
            "package_name": None,
            "app_name": "Nubank",
            "text": "Prejuízo referente a venda de 5 cotas de Apple Inc. no preço de venda de 30 Reais",
            "user_id": 1,
            "last_update": "2026-07-21",
            "category": "Prejuízo investimentos",
            "key": "AAPL_2026-07-21_5.0_42_mock-req-123",
            "copy": False,
            "request_id": "mock-req-123",
            "is_installment": "N",
            "installment_id": None,
        }
        self.remote_repository.insert.assert_any_call(
            "transactions", expected_prejuizo_tx, headers
        )

    @patch("service.investment_handler.Utils.inform_to_client")
    @patch.object(InvestmentHandler, "add_profit_loss")
    def test_add_movement_calls_add_profit_loss(
        self, mock_add_profit_loss, mock_inform_to_client
    ):
        # Setup mocks for add_movement
        movement = {
            "movement_type": 2,
            "ticker": "AAPL",
            "quantity": 10.0,
            "price": 50.0,
            "date": "2026-07-21",
        }
        headers = {"userName": "test_user"}

        movement_with_user_id = {
            "movement_type": 2,
            "ticker": "AAPL",
            "quantity": 10.0,
            "price": 50.0,
            "date": "2026-07-21",
            "user_id": 1,
        }

        self.remote_repository.add_user_id.return_value = movement_with_user_id

        movement_type_mock = {"id": 2, "coefficient": -1, "to_balance": True}

        stock_mock = {
            "id": 42,
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "investment_type_id": 1,
        }

        user_stock_mock = {
            "user_id": 1,
            "investment_id": 42,
            "quantity": 20.0,
            "avg_price": 30.0,
            "investment_type_id": 1,
        }

        inserted_movement_mock = {
            "id": 1001,
            "movement_type": 2,
            "quantity": 10.0,
            "price": 50.0,
            "date": "2026-07-21",
            "user_id": 1,
            "investment_id": 42,
            "investment_type_id": 1,
        }

        # Setup side effects
        def get_object_side_effect(table, keys=None, data=None, hdrs=None):
            if table == "movement_types":
                return movement_type_mock
            elif table == "user_stocks":
                return user_stock_mock
            elif table == "stocks":
                return stock_mock
            return None

        self.remote_repository.get_object.side_effect = get_object_side_effect
        self.remote_repository.exist_by_key.return_value = True

        def insert_side_effect(table, data, hdrs):
            if table == "user_stocks_movements":
                return inserted_movement_mock
            return data

        self.remote_repository.insert.side_effect = insert_side_effect

        # Call method
        result = self.handler.add_movement(movement, headers)

        # Assertions
        self.assertEqual(result["status"], "success")

        # Verify user_stocks update occurred
        self.remote_repository.update.assert_called_with(
            "user_stocks", ["user_id", "investment_id"], user_stock_mock, headers
        )

        # Verify add_profit_loss was called with correct parameters:
        # profit_loss_value = movement['price'] - user_stock['avg_price'] = 50.0 - 30.0 = 20.0
        # and inserted_movement_mock (the result of user_stocks_movements insert)
        mock_add_profit_loss.assert_called_once_with(
            20.0, inserted_movement_mock, headers
        )


if __name__ == "__main__":
    unittest.main()
