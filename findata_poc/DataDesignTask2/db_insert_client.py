import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import date
from dataclasses import asdict
from typing import List, Optional
from json_decoder import Client, JsonDecoder  


class DatabaseHandler:
    """
    Handles the storage of financial data (Client objects) into a PostgreSQL database.
    """

    def __init__(self, connection_string: str):
        """
        Initializes the DatabaseHandler with a PostgreSQL connection string.

        Args:
            connection_string: The connection string for the PostgreSQL database.
        """
        self.connection_string = connection_string
        self.conn = None  # Initialize connection to None
        self.cur = None

    def _connect(self):
        """
        Establishes a connection to the PostgreSQL database.
        Handles connection errors gracefully.  Returns True if successful, False otherwise.
        """
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cur = self.conn.cursor()
            return True
        except psycopg2.OperationalError as e:
            print(f"Failed to connect to PostgreSQL database: {e}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during connection: {e}")
            return False

    def _close(self):
        """
        Closes the database connection and cursor.
        """
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        self.conn = None  # Reset connection and cursor after closing
        self.cur = None

    def _insert_client(self, client: Client, partner_id: str, data_date: date):
        """Inserts client data into the Clients table."""
        insert_client = """
            INSERT INTO financial_data.Clients (client_id, partner_id, data_date, name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        self.cur.execute(insert_client, (client.client_id, partner_id, data_date, client.name))

    def _insert_accounts(self, client: Client, partner_id: str, data_date: date):
        """Inserts account data into the Accounts table."""
        if not client.accounts:
            return

        account_data = [
            (
                account.account_id,
                client.client_id,
                partner_id,
                data_date,
                account.value,
                account.currency,
                account.name,
                account.type,
            )
            for account in client.accounts
        ]

        insert_account = """
            INSERT INTO financial_data.Accounts 
            (account_id, client_id, partner_id, data_date, value, currency, name, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        execute_batch(self.cur, insert_account, account_data)

    def _insert_holdings(self, client: Client, partner_id: str, data_date: date):
        """Inserts holding data into the Holdings table."""
        if not client.holdings:
            return

        holding_data = [
            (
                holding.holding_id,
                holding.account_id,
                partner_id,
                data_date,
                holding.name,
                holding.security,
                holding.quantity,
                holding.buy_price,
                holding.is_cash_like,
            )
            for holding in client.holdings
        ]

        insert_holding = """
            INSERT INTO financial_data.Holdings 
            (holding_id, account_id, partner_id, data_date, name, security, quantity, buy_price, is_cash_like)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        execute_batch(self.cur, insert_holding, holding_data)

    def _insert_transactions(self, client: Client, partner_id: str, data_date: date):
        """Inserts transaction data into the Transactions table."""
        if not client.transactions:
            return

        transaction_data = [
            (
                transaction.transaction_id,
                transaction.account_id,
                transaction.holding_id,
                partner_id,
                data_date,
                transaction.type,
                transaction.quantity,
                transaction.value,
                transaction.date.isoformat() if transaction.date else None,
                transaction.settle_date.isoformat() if transaction.settle_date else None,
            )
            for transaction in client.transactions
        ]

        insert_transaction = """
            INSERT INTO financial_data.Transactions 
            (transaction_id, account_id, holding_id, partner_id, data_date, type, quantity, value, date, settle_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        execute_batch(self.cur, insert_transaction, transaction_data)


    def store_client_data(self, client: Client, partner_id: str, data_date: date):
        """
        Stores the client data in the database.  This is the main entry point.

        Args:
            client: The Client object to store.
            partner_id: The ID of the partner associated with the client.
            data_date: The date for the data being stored.
        """
        if not self._connect():  # Attempt to connect, and exit if it fails.
            return

        try:
            self._insert_client(client, partner_id, data_date)
            self._insert_accounts(client, partner_id, data_date)
            self._insert_holdings(client, partner_id, data_date)
            self._insert_transactions(client, partner_id, data_date)
            self.conn.commit()
            print("Client data successfully stored in PostgreSQL tables.")

        except psycopg2.Error as e:
            print(f"Error while storing data: {e}")
            if self.conn:
                self.conn.rollback()  # Rollback in case of error
            
        finally:
            self._close()  # Always close the connection


# Example usage (assuming you have a CONNECTION_STRING and sample JSON data)
if __name__ == "__main__":
    CONNECTION_STRING = "postgresql://appuser:your_password@localhost:5432/financial_db"

    # Sample JSON data (replace with your actual data)    
    sample_json_data = """
    {
        "id": "c_1234",
        "name": "John Adams",
        "accounts": [
            {
                "id": "a_1234",
                "value": "12098",
                "currency": "USD",
                "name": "Brokerage",
                "type": "Brokerage"
            },
            {
                "id": "a_2345",
                "value": "1045",
                "currency": "CAD",
                "name": "John's Retirement",
                "type": "IRA"
            }
        ],
        "holdings": [
            {
                "id": "h_1234",
                "accountId": "a_1234",
                "name": "Apple Inc",
                "security": "AAPL",
                "quantity": 14.5,
                "buyPrice": 145,
                "isCashLike": false
            },
            {
                "id": "h_2345",
                "accountId": "a_1234",
                "name": "Apple Bond 2026 6%",
                "security": null,
                "quantity": 140,
                "buyPrice": 0.98,
                "isCashLike": false
            }
        ],
        "transactions": [
            {
                "id": "t_1234",
                "accountId": "a_1234",
                "holdingId": "h_1234",
                "type": "SELL",
                "quantity": 2,
                "value": 167,
                "date": "2024-04-13",
                "settleDate": "2024-04-15"
            }
        ]
    }
    """
    
    # Load and decode data
    try:
        client = JsonDecoder.decode(sample_json_data) # Use your JsonDecoder
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print("An unexpected error occurred:", e)
    else:
        # Store the data in the database
        db_handler = DatabaseHandler(CONNECTION_STRING)
        db_handler.store_client_data(client, "partner789", date.today())