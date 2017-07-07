DROP TABLE IF EXISTS srf_contracts;
DROP TABLE IF EXISTS srf_data;
DROP TABLE IF EXISTS srf_products;

CREATE TABLE SRF_PRODUCTS (
		ID				INT 					NOT NULL,
		SYMBOL		TEXT 					NOT NULL,
		CATEGORY		TEXT					NOT NULL,
		EXCHANGE		TEXT					NOT NULL,
		MONTH			TEXT					DEFAULT 'FGHJKMNQUVXZ',
		DESCRIPTION	TEXT					DEFAULT '',
		CONSTRAINT	PRIM_KEY	PRIMARY KEY (ID)
);

CREATE TABLE SRF_DATA (
		DATE			DATE 					NOT NULL,
		ID				INT  					NOT NULL,
		EXP			CHAR(5)				NOT NULL,
		OPEN			NUMERIC				(16, 4),
		HIGH			NUMERIC				(16, 4),
		LOW			NUMERIC				(16, 4),
		CLOSE			NUMERIC				(16, 4),
		SETTLE		NUMERIC				(16, 4),
		VOL			NUMERIC				(16, 4),
		OPI			NUMERIC				(16, 4),
		PnL			NUMERIC				(16, 6),
		LogR			NUMERIC				(16, 6),
		CONSTRAINT PRIM_KEY2 PRIMARY KEY (DATE, ID, EXP),
		CONSTRAINT FORI_KEY2 FOREIGN KEY (ID) REFERENCES SRF_PRODUCTS (ID)
);

CREATE TABLE SRF_CONTRACTS (
		ID				INT  					NOT NULL,
		EXP		CHAR(5)				NOT NULL,
		START			DATE					DEFAULT NULL,	
		LAST			DATE					DEFAULT NULL,	
		ROLL			DATE					DEFAULT NULL,	
		CHAIN			INT					DEFAULT 0,	
		CONSTRAINT PRIM_KEY3 PRIMARY KEY (ID, EXP)
);



INSERT INTO SRF_PRODUCTS (ID, SYMBOL, CATEGORY, EXCHANGE, MONTH, DESCRIPTION) VALUES
	(1001, 'DJ', 'Equity Index', 'CBOT', 'HMUZ', 'CBOT Dow Jones Ind Avg (DJIA)'),
	(1002, 'NQ', 'Equity Index', 'CME',  'HMUZ', 'CME NASDAQ 100 Index Mini'),
	(1003, 'NK', 'Equity Index', 'CME',  'HMUZ', 'CME Nikkei 225'),
	(1004, 'MD', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 400 MidCap Index'),
	(1005, 'SP', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 500 Index'),
	(1006, 'ES', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 500 Index E-Mini'),
	(1007, 'RF', 'Equity Index', 'ICE',  'HMUZ', 'ICE Russell 1000 Index Mini'),
	(1008, 'TF', 'Equity Index', 'ICE',  'HMUZ', 'ICE Russell 2000 Index Mini'),

	(2001, 'TY', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 10-year US Treasury Note'),
	(2002, 'FV', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 5-year US Treasury Note'),
	(2003, 'TU', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 2-year US Treasury Note'),
	(2004, 'FF', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 30-day US Treasury Note'),
	(2005, 'US', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 30-year US Treasury Note'),
	(2011, 'ED', 'Interest Rates', 'CME',  'HJKMNQUZ', 'CME Eurodollar'),

	(3001, 'AD', 'Forex', 'CME', 'HMUZ', 'CME Australian Dollar AUD'),
	(3002, 'BP', 'Forex', 'CME', 'HMUZ', 'CME British Pound GBP'),
	(3003, 'CD', 'Forex', 'CME', 'HMUZ', 'CME Canadian Dollar CAD'),
	(3004, 'EC', 'Forex', 'CME', 'HMUZ', 'CME Euro FX'),
	(3005, 'JY', 'Forex', 'CME', 'HMUZ', 'CME Japanese Yen JPY'),
	(3006, 'NE', 'Forex', 'CME', 'HMUZ', 'CME New Zealand Dollar NZD'),
	(3007, 'SF', 'Forex', 'CME', 'HMUZ', 'CME Swiss Franc CHF'),
	(3008, 'MP', 'Forex', 'ICE', 'HMUZ', 'ICE British Pound GBP'),
	(3009, 'DX', 'Forex', 'ICE', 'HMUZ', 'ICE US Dollar Index'),

	(4001, 'RB',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Gasoline'),
	(4002, 'HO',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Heating Oil'),
	(4003, 'NG',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Natural Gas'),
	(4004, 'CL',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX WTI Crude Oil'),
	(4005, 'B',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Brent Crude Oil'),
	(4006, 'G',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Gasoil'),
	(4007, 'O',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Heating Oil'),
	(4008, 'ATW',  'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Rotterdam Coal'),
	(4009, 'M',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE UK Natural Gas'),
	(4010, 'T',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE WTI Crude Oil'),

	(5001, 'GC', 'Metals', 'NYMEX', 'GHJKMQVZ', 		'NYMEX Glod'),
	(5002, 'PA', 'Metals', 'NYMEX', 'HJKMUZ', 		'NYMEX Palladium'),
	(5003, 'PL', 'Metals', 'NYMEX', 'FHJKNV', 		'NYMEX Platinum'),
	(5004, 'SI', 'Metals', 'NYMEX', 'FHJKNUZ', 		'NYMEX Silver'),
	(5005, 'HG', 'Metals', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Copper'),

	(8001, 'C',  'Grains', 'CBOT', 'HKNUZ',    'CBOT Corn'),
	(8002, 'SM', 'Grains', 'CBOT', 'FHKNQUVZ', 'CBOT Soybean Meal'),
	(8003, 'BO', 'Grains', 'CBOT', 'FHKNQUVZ', 'CBOT Soybean Oil'),
	(8004, 'S',  'Grains', 'CBOT', 'FHKNQUX',  'CBOT Soybeans'),
	(8005, 'W',  'Grains', 'CBOT', 'HKNUZ',    'CBOT Wheat'),
	(8006, 'KW', 'Grains', 'CME',  'HKNUZ',    'CME Kansas City Wheat'),
	(8011, 'CC', 'Grains', 'ICE',  'HKNQUVXZ', 'ICE Cocoa'),
	(8012, 'KC', 'Grains', 'ICE',  'HKNUZ',    'ICE Coffee C'),
	(8013, 'CT', 'Grains', 'ICE',  'FHKNUVXZ', 'ICE Cotton'),
	(8014, 'OJ', 'Grains', 'ICE',  'FHKNUX',   'ICE Orange Juice'),
	(8015, 'SB', 'Grains', 'ICE',  'FHKNQUVX', 'ICE Sugar No. 11'),

	(9001, 'LN', 'Livestock', 'CME', 'GJKMNQVZ', 'CME Lean Hogs'),
	(9002, 'LC', 'Livestock', 'CME', 'GJMQVZ', 'CME Live Cattle')
;
