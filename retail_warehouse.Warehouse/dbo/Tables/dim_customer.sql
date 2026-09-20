CREATE TABLE [dbo].[dim_customer] (
    [CustomerKey]     INT          NOT NULL,
    [CustomerID]      VARCHAR (50) NULL,
    [Gender]          VARCHAR (10) NULL,
    [Region]          VARCHAR (50) NULL,
    [City]            VARCHAR (50) NULL,
    [Age]             INT          NULL,
    [CustomerSegment] VARCHAR (20) NULL,
    [SignUpDate]      DATE         NULL
);


GO

ALTER TABLE [dbo].[dim_customer]
    ADD CONSTRAINT [PK_dim_customer] PRIMARY KEY NONCLUSTERED ([CustomerKey] ASC) NOT ENFORCED;


GO