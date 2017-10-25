/*
 * Integrated schema design:
 *
 * - Calendar table is same as current customer database calendar table
 * - Reviews table summarizes key information from product reviews JSON file
 * - Categories flattens the nested structure of ClassificationInfo JSON file
 * - Sales table integrates information from current customer database tables
 *   products, orders, orderlines, customers, campaigns, and classificationInfo
 */

CREATE TABLE Calendar (
	Date date NOT NULL PRIMARY KEY,
	ISO varchar(10) NOT NULL,
	datenum int NOT NULL,
	DOW char(3) NOT NULL,
	DOWint smallint NOT NULL,
	Year smallint NOT NULL,
	Month smallint NOT NULL,
	DOM smallint NOT NULL,
	MonthAbbr char(3) NOT NULL,
	DOY smallint NOT NULL,
	Mondays smallint NOT NULL,
	Tuesdays smallint NOT NULL,
	Wednesdays smallint NOT NULL,
	Thursdays smallint NOT NULL,
	Fridays smallint NOT NULL,
	Saturdays smallint NOT NULL,
	Sundays smallint NOT NULL,
	NumHolidays int NOT NULL,
	HolidayName varchar(255) NULL,
	HolidayType varchar(9) NULL,
	hol_National varchar(255) NULL,
	hol_Minor varchar(255) NULL,
	hol_Christian varchar(255) NULL,
	hol_Jewish varchar(255) NULL,
	hol_Muslim varchar(255) NULL,
	hol_Chinese varchar(255) NULL,
	hol_Other varchar(255) NULL
);

CREATE TABLE Reviews (
    Asin int PRIMARY KEY NOT NULL,
    NumReviews int NOT NULL,
    NumHelpful int NOT NULL,
    AvgRating decimal NOT NULL,
    AvgReviewLength decimal NOT NULL
);

CREATE TABLE Categories (	
	Classification varchar(50) PRIMARY KEY NOT NULL,
	Category0 varchar(50),
	Category1 varchar(50),
	Category2 varchar(50),
	Category3 varchar(50),
	Category4 varchar(50),
	Category5 varchar(50),
	AvgProductPrice money
);

/* 
 * Or, the Categories table can be structured as below: 

CREATE TABLE Categories (	
	Classification varchar(50) NOT NULL,
	CategoryName varchar(50) NOT NULL,
	CategoryLevel int NOT NULL,
	AvgProductPrice money
);
 */

CREATE TABLE Sales (
	SalesId int PRIMARY KEY NOT NULL,
	OrderDate date REFERENCES Calendar (date) NOT NULL,
	Asin int REFERENCES Reviews (asin) NOT NULL,
	ProductCat varchar(50) REFERENCES Categories (Classification) NOT NULL,
	ProductId int NOT NULL,
	NumUnits int NOT NULL,
	FullPrice money NOT NULL,
	City varchar(50) NOT NULL,
	State varchar(50) NOT NULL,
	ZipCode varchar(50) NOT NULL,
	CampaignFlag char(1) NOT NULL,
	CampaignChannel varchar(50),
	CampaignDiscount int NOT NULL,
	CampaignFreeShppingFlag char(1) NOT NULL,
	CustomerGender varchar(50) NOT NULL
);