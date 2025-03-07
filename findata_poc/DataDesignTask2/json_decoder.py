import json
from dataclasses import dataclass
from typing import List, Optional
import datetime



@dataclass(frozen=True)
class Account:
    account_id: str
    value: float  
    currency: str
    name: str
    type: str

    @classmethod
    def from_dict(cls, data: dict) -> "Account":
        return cls(
            account_id=data.get("id"),
            value=float(data.get("value")),  # Convert value to float
            currency=data.get("currency"),
            name=data.get("name"),
            type=data.get("type")
        )

@dataclass(frozen=True)
class Holding:
    holding_id: str
    account_id: str
    name: str
    security: Optional[str]
    quantity: float
    buy_price: float
    is_cash_like: bool

    @classmethod
    def from_dict(cls, data: dict) -> "Holding":
        # Convert isCashLike if it's a string
        isCashLike = data.get("isCashLike")
        if isinstance(isCashLike, str):
            isCashLike = isCashLike.lower() == "true"
        return cls(
            holding_id=data.get("id"),
            account_id=data.get("accountId"),
            name=data.get("name"),
            security=data.get("security"),
            quantity=float(data.get("quantity")),
            buy_price=float(data.get("buyPrice")),
            is_cash_like=isCashLike
        )


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    account_id: str        
    holding_id: Optional[str]  
    type: str
    quantity: float
    value: float
    date: datetime.date
    settle_date: datetime.date

    @classmethod
    def from_dict(cls, data: dict) -> "Transaction":
        # Parse dates from the "YYYY-MM-DD" format
        date_str = data.get("date")
        settle_date_str = data.get("settleDate")
        date_parsed = datetime.datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
        settle_date_parsed = datetime.datetime.strptime(settle_date_str, "%Y-%m-%d").date() if settle_date_str else None

        return cls(
            transaction_id=data.get("id"),
            account_id=data.get("accountId"),
            holding_id=data.get("holdingId"),  # Can be None
            type=data.get("type"),
            quantity=float(data.get("quantity")),
            value=float(data.get("value")),
            date=date_parsed,
            settle_date=settle_date_parsed
        )

@dataclass(frozen=True)
class Client:
    client_id: str
    name: str
    accounts: List[Account]
    holdings: List[Holding]
    transactions: List[Transaction]

    @classmethod
    def from_dict(cls, data: dict) -> "Client":
        accounts = [Account.from_dict(a) for a in data.get("accounts", [])]
        holdings = [Holding.from_dict(h) for h in data.get("holdings", [])]
        transactions = [Transaction.from_dict(t) for t in data.get("transactions", [])]
        return cls(
            client_id=data.get("id"),
            name=data.get("name"),
            accounts=accounts,
            holdings=holdings,
            transactions=transactions
        )

class JsonDecoder:
    @staticmethod
    def decode(json_string: str) -> Client:
        data = json.loads(json_string)  
        return Client.from_dict(data)


# Example usage:
if __name__ == "__main__":
    sample_json = '''{
        "id": "c_1234",
        "name": "John Adams",
        "accounts": [{
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
        "holdings": [{
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
        "transactions": [{
            "id": "t_1234",
            "accountId": "a_1234",
            "holdingId": "h_1234",
            "type": "SELL",
            "quantity": 2,
            "value": 167,
            "date": "2024-04-13",
            "settleDate": "2024-04-15"
        }]
    }'''

    client = JsonDecoder.decode(sample_json)
    print(client)
