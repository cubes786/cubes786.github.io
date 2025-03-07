import json
from dataclasses import dataclass, replace, asdict
from typing import List, Optional
import datetime
from flask import Flask, jsonify
from json_decoder_FP import Client, JsonDecoder, Holding

# --- Provided SDK Pricing Function ---
def getPrice(security: str) -> int:
    # Dummy implementation for demonstration.
    return 100


# --- EnhanceHoldings Class ---
class EnhanceHoldings:
    """
    Encapsulates the functional transformation that attaches current market pricing.
    When instantiated with a list of holdings, it returns a list of dictionaries
    with the pricing data attached under the key 'currentPrice'.
    """

    @staticmethod
    def enhance_holding(holding: Holding) -> dict:
        # Only attempt to get a price if the holding has an associated security.
        price = getPrice(holding.security) if holding.security is not None else None
        # Convert the holding to a dict and attach the currentPrice.
        data = asdict(holding)
        data["currentPrice"] = price
        return data

    @staticmethod
    def enhance_holdings(holdings: List[Holding]) -> List[dict]:
        return list(map(EnhanceHoldings.enhance_holding, holdings))

    def __new__(cls, holdings: List[Holding]) -> List[dict]:
        return cls.enhance_holdings(holdings)

# --- Flask Endpoint ---
app = Flask(__name__)

# For demonstration, we load a sample client from JSON.
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
            "name": "Depository Sweep",
            "security": null,
            "quantity": 1598.96,
            "buyPrice": 1,
            "isCashLike": true
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

@app.route("/holdings_with_price", methods=["GET"])
def holdings_with_price():
    # Enhance each holding with current market pricing using the functional transformer.
    enhanced = EnhanceHoldings(client.holdings)
    return jsonify(enhanced)

if __name__ == "__main__":
    app.run(debug=True)
