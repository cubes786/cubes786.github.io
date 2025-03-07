SELECT * FROM financial_data.clients;
SELECT * FROM financial_data.accounts; 
SELECT * FROM financial_data.holdings;
SELECT * FROM financial_data.transactions;


TRUNCATE TABLE financial_data.transactions, financial_data.clients, financial_data.accounts, financial_data.holdings CASCADE;