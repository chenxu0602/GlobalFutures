DROP TABLE IF EXISTS contracts;
DROP TABLE IF EXISTS daily_data;
DROP TABLE IF EXISTS products;

CREATE TABLE PRODUCTS (
		ID				INT 					NOT NULL,
		SYMBOL		TEXT 					NOT NULL,
		CATEGORY		TEXT					NOT NULL,
		EXCHANGE		TEXT					NOT NULL,
		MONTH			TEXT					DEFAULT 'FGHJKMNQUVXZ',
		DESCRIPTION	TEXT					DEFAULT '',
		SOURCE		TEXT					DEFAULT '',
		CONSTRAINT	PRIM_KEY	PRIMARY KEY (ID)
);

CREATE TABLE DAILY_DATA (
		DATE			DATE 					NOT NULL,
		ID				INT  					NOT NULL,
		EXP			CHAR(5)				NOT NULL,
		CHAIN			INT					DEFAULT 0,	
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
		CONSTRAINT FORI_KEY2 FOREIGN KEY (ID) REFERENCES PRODUCTS (ID)
);

CREATE TABLE CONTRACTS (
		ID				INT  					NOT NULL,
		EXP		CHAR(5)				NOT NULL,
		START			DATE					DEFAULT NULL,	
		LAST			DATE					DEFAULT NULL,	
		ROLL			DATE					DEFAULT NULL,	
		CONSTRAINT PRIM_KEY3 PRIMARY KEY (ID, EXP)
);



INSERT INTO PRODUCTS (ID, SYMBOL, CATEGORY, EXCHANGE, MONTH, DESCRIPTION, SOURCE) VALUES
	(1001, 'DJ', 'Equity Index', 'CBOT', 'HMUZ', 'CBOT Dow Jones Ind Avg (DJIA)', 'SRF'),
	(1002, 'NQ', 'Equity Index', 'CME',  'HMUZ', 'CME NASDAQ 100 Index Mini', 	 	'SRF'),
	(1003, 'NK', 'Equity Index', 'CME',  'HMUZ', 'CME Nikkei 225', 					'SRF'),
	(1004, 'MD', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 400 MidCap Index', 	 	'SRF'),
	(1005, 'SP', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 500 Index', 				'SRF'),
	(1006, 'ES', 'Equity Index', 'CME',  'HMUZ', 'CME S&P 500 Index E-Mini', 	 	'SRF'),
	(1007, 'RF', 'Equity Index', 'ICE',  'HMUZ', 'ICE Russell 1000 Index Mini', 	'SRF'),
	(1008, 'TF', 'Equity Index', 'ICE',  'HMUZ', 'ICE Russell 2000 Index Mini', 	'SRF'),

	(2001, 'TY', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 10-year US Treasury Note', 'SRF'),
	(2002, 'FV', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 5-year US Treasury Note',  'SRF'),
	(2003, 'TU', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 2-year US Treasury Note',  'SRF'),
	(2004, 'FF', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 30-day US Treasury Note',  'SRF'),
	(2005, 'US', 'Interest Rates', 'CBOT', 'HMUZ',     'CBOT 30-year US Treasury Note', 'SRF'),
	(2011, 'ED', 'Interest Rates', 'CME',  'HJKMNQUZ', 'CME Eurodollar',                'SRF'),

	(3001, 'AD',  'Forex', 'CME', 'HMUZ', 			'CME Australian Dollar AUD',  'SRF'),
	(3002, 'BP',  'Forex', 'CME', 'HMUZ', 			'CME British Pound GBP',      'SRF'),
	(3003, 'CD',  'Forex', 'CME', 'HMUZ', 			'CME Canadian Dollar CAD',    'SRF'),
	(3004, 'EC',  'Forex', 'CME', 'HMUZ', 			'CME Euro FX',                'SRF'),
	(3005, 'JY',  'Forex', 'CME', 'HMUZ', 			'CME Japanese Yen JPY',       'SRF'),
	(3006, 'NE',  'Forex', 'CME', 'HMUZ', 			'CME New Zealand Dollar NZD', 'SRF'),
	(3007, 'SF',  'Forex', 'CME', 'HMUZ', 			'CME Swiss Franc CHF',        'SRF'),
	(3008, 'MP',  'Forex', 'CME', 'FGHJKMNQUVXZ', 'CME Mexican Peso',   			'SRF'),
--	(3009, 'MP',  'Forex', 'ICE', 'HMUZ', 			'ICE British Pound GBP',      'SRF'),
	(3010, 'DX',  'Forex', 'ICE', 'HMUZ', 			'ICE US Dollar Index',        'SRF'),
	(3011, 'RU',  'Forex', 'CME', 'FGHJKMNQUVXZ', 'Russian Ruble',      			'CME'),
	(3012, 'RA',  'Forex', 'CME', 'FGHJKMNQUVXZ', 'South African Rand',      	'CME'),
	(3013, 'CNH', 'Forex', 'CME', 'FGHJKMNQUVXZ', 'USD/Offshore RMB',      		'CME'),

	(4001, 'RB',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Gasoline',      'SRF'),
	(4002, 'HO',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Heating Oil',   'SRF'),
	(4003, 'NG',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Natural Gas',   'SRF'),
	(4004, 'CL',   'Energy', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX WTI Crude Oil', 'SRF'),
	(4005, 'B',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Brent Crude Oil', 'SRF'),
	(4006, 'G',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Gasoil',          'SRF'),
	(4007, 'O',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Heating Oil',     'SRF'),
	(4008, 'ATW',  'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE Rotterdam Coal',  'SRF'),
	(4009, 'M',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE UK Natural Gas',  'SRF'),
	(4010, 'T',    'Energy', 'ICE',   'FGHJKMNQUVXZ', 'ICE WTI Crude Oil',   'SRF'),

	(5001, 'GC', 'Metals', 'NYMEX', 'GHJKMQVZ', 		'NYMEX Glod',      'SRF'),
	(5002, 'PA', 'Metals', 'NYMEX', 'HJKMUZ', 		'NYMEX Palladium', 'SRF'),
	(5003, 'PL', 'Metals', 'NYMEX', 'FHJKNV', 		'NYMEX Platinum',  'SRF'),
	(5004, 'SI', 'Metals', 'NYMEX', 'FHJKNUZ', 		'NYMEX Silver',    'SRF'),
	(5005, 'HG', 'Metals', 'NYMEX', 'FGHJKMNQUVXZ', 'NYMEX Copper',    'SRF'),

	(6001, 'CU', 'Chemicals', 'NYMEX', 'FGHJKMNQUVXZ', 'Chicago Ethanol (Platts)', 'CME'),

	(8001, 'C',  'Grains', 'CBOT', 'HKNUZ',    'CBOT Corn',             'SRF'),
	(8002, 'SM', 'Grains', 'CBOT', 'FHKNQUVZ', 'CBOT Soybean Meal',     'SRF'),
	(8003, 'BO', 'Grains', 'CBOT', 'FHKNQUVZ', 'CBOT Soybean Oil',      'SRF'),
	(8004, 'S',  'Grains', 'CBOT', 'FHKNQUX',  'CBOT Soybeans',         'SRF'),
	(8005, 'W',  'Grains', 'CBOT', 'HKNUZ',    'CBOT Wheat',            'SRF'),
	(8006, 'KW', 'Grains', 'CME',  'HKNUZ',    'CME Kansas City Wheat', 'SRF'),
	(8011, 'CC', 'Grains', 'ICE',  'HKNQUVXZ', 'ICE Cocoa',             'SRF'),
	(8012, 'KC', 'Grains', 'ICE',  'HKNUZ',    'ICE Coffee C',          'SRF'),
	(8013, 'CT', 'Grains', 'ICE',  'FHKNUVXZ', 'ICE Cotton',            'SRF'),
	(8014, 'OJ', 'Grains', 'ICE',  'FHKNUX',   'ICE Orange Juice',      'SRF'),
	(8015, 'SB', 'Grains', 'ICE',  'FHKNQUVX', 'ICE Sugar No. 11',      'SRF'),

	(9001, 'LN', 'Livestock', 'CME', 'GJKMNQVZ', 'CME Lean Hogs', 	 'SRF'),
	(9002, 'LC', 'Livestock', 'CME', 'GJMQVZ',   'CME Live Cattle', 'SRF'),
	(9003, 'FC', 'Livestock', 'CME', 'FHJKQUVX', 'CME Live Cattle', 'CME')
;
