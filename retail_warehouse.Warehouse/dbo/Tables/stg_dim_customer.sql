CREATE TABLE [dbo].[stg_dim_customer] (
    [CustomerID]      VARCHAR (50) NULL,
    [Gender]          VARCHAR (10) NULL,
    [Region]          VARCHAR (50) NULL,
    [City]            VARCHAR (50) NULL,
    [Age]             INT          NULL,
    [CustomerSegment] VARCHAR (20) NULL,
    [SignUpDate]      DATE         NULL
);


GO